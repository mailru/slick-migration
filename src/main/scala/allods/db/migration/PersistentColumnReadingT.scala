package allods.db.migration

import slick.dbio.DBIO
import slick.jdbc.ActionBasedSQLInterpolation
import slick.jdbc.meta._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.implicitConversions
import scala.concurrent._

/**
  * A mechanism that reads signuficant parts of schema and some means to compare two schemas.
  * The scheme is represented with a tree with labelled branches.
  */
trait PersistentColumnReadingT extends TreeSchemaT {

  implicit def ActionBasedSQLInterpolationImport(s: StringContext): ActionBasedSQLInterpolation = new ActionBasedSQLInterpolation(s)

  /** Load function definitions from schemaName. */
  def loadFunctionsDbio(schemaName: String)(implicit executionContext: ExecutionContext): DBIO[Vector[(String, String)]] =
    sql"""SELECT proc.proname, proc.proargnames, proc.prosrc
         FROM pg_proc AS proc
         JOIN pg_namespace AS nsp ON nsp.oid = proc.pronamespace
         WHERE nsp.nspname LIKE $schemaName""".as[(String, String, String)].
      map(_.map { case (name, args, body) => name + "(" + args + ")" -> lowerCaseAndReplaceBlank(body) })

  def loadViewsDbio(schemaName: String)(implicit executionContext: ExecutionContext): DBIO[Vector[(String, String)]] = {
    sql"""SELECT viewname,
                 pg_get_viewdef(schemaname||'.'||viewname, TRUE) AS definition
           FROM pg_views
           WHERE schemaname LIKE $schemaName""".as[(String, String)].
      map(_.map { case (name, body) => name -> body })

  }

  /**
   * Normalize function body.
   * As an alternative one may use:
   * {{{
   *     def normalizeFunctionBody(body: String) = body.replaceAll("\\s{1,}", " ").trim.toLowerCase
   * }}}
   * @param source function definition
   * @return normalized function definition.
   */
  def lowerCaseAndReplaceBlank(source: String) = source.replaceAll("\\s", "").toLowerCase

  /** A column representation that can be directly compared.*/
  case class PersistentColumn(name: String, columnDef: Option[String], sqlType: Int, typeName: String,
                              size: Option[Int])

  val toPersistentColumn = { (c: MColumn) =>
    import c._
    PersistentColumn(name, columnDef, sqlType, typeName, size)
  }

  /** Load trigger list for the tableName. */
  def triggersDbio(tableName: MQName): DBIO[(String, Node)] =
    sql"""
         SELECT tgname
         FROM pg_trigger
         JOIN pg_class ON tgrelid = pg_class.oid
         JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
         WHERE pg_namespace.nspname LIKE ${tableName.schema.getOrElse("public")}
           AND pg_class.relname LIKE ${tableName.name}
           AND NOT tgisinternal
      """.as[String].map { triggers =>
      "trigger" -> ParentNode(triggers.map { case name => quote(name) -> ContentNode("") }.toMap)
    }

  /** Table name format.*/
  def MQNameToString(name:MQName) =
    name.schema.map(_ + ".").getOrElse("")  + name.name

  /** Load inherited tables. */
  def inheritsDbio(table: MTable): DBIO[Seq[(String, Node)]] =
    if (table.tableType == "TABLE")
      sql"""
      SELECT inhparent::REGCLASS::TEXT
      FROM pg_catalog.pg_inherits
      WHERE inhrelid  = ${MQNameToString(table.name)}::REGCLASS::OID
      """.as[String].map { inherits =>
        Seq("inherit" -> ParentNode(inherits.map { case name => quote(name) -> ContentNode("") }.toMap))
      }
    else
      DBIO.successful(Seq())

  /** Create quoted name.*/
  def quote(name: String) = "\"" + name + "\""

  /** Load function definitions. */
  def functionsDbio(schema: String): DBIO[(String, Node)] =
    loadFunctionsDbio(schema).map(functions => "function" -> ParentNode(functions.map { case (name, body) =>
      quote(name) -> ContentNode(body)
    }.toMap))

  def columnsDbio(table: MTable): DBIO[(String, Node)] =
    table.getColumns.map(columns => "column" -> ParentNode(columns.map(column =>
      quote(column.name) -> ContentNode(toPersistentColumn(column).toString)
    ).toMap))

  def tablesDbio(schema: String): DBIO[(String, Node)] =
    MTable.getTables(None, Some(schema), None, None).
      flatMap { mtables =>
        DBIO.sequence(mtables.map(mtable =>
          DBIO.sequence(Seq(
            columnsDbio(mtable),
            triggersDbio(mtable.name)
          )).flatMap(s1 =>
            inheritsDbio(mtable).
              map(s2 => s1 ++ s2)
          ).map(_.toMap).
          map { m =>
            mtable.tableType + " " + quote(mtable.name.name) -> ParentNode(m)
          }
        )).map(ns => "table" -> ParentNode(ns.toMap))
      }

  def viewsDbio(schema: String): DBIO[(String, Node)] =
    loadViewsDbio(schema).map(views => "view" -> ParentNode(views.map {
      case (name, body) =>
        quote(name) -> ContentNode(body)
    }.toMap))

  /** Load the essential DB schema. */
  def readEssentialSchemaDbio(dbExcludedSchemas: Set[String])(implicit executionContext: ExecutionContext): DBIO[Node] = {
    MSchema.getSchemas(None, None).flatMap {
      schemas =>
        DBIO.sequence(schemas.filter(s => !dbExcludedSchemas.contains(s.schema)).map {
          schema =>
            DBIO.sequence(Seq(
              functionsDbio(schema.schema),
              tablesDbio(schema.schema),
              viewsDbio(schema.schema)
            )).map(_.toMap).map {
              m => schema.schema -> ParentNode(m)
            }
        })
    }.map {
      p =>
        ParentNode(Map("schema" -> ParentNode(p.toMap)))
    }.withPinnedSession
  }


}
