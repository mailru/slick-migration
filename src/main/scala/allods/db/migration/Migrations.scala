package allods.db.migration

import java.sql.Timestamp
import javax.sql.DataSource

import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.DDL
import slick.driver.PostgresDriver.api._
import slick.jdbc.meta.MTable

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext//.Implicits.global

/**
 * Support for migration of database. objects.
 *
 * Схема БД может быть разбита на отдельные независимые подсхемы, располагающиеся в одной БД (при условии,
 * что подсхемы не пересекаются по отдельным объектам БД). Например, схема шарда, схема логсервера и т.п.
 * Внутри подсхемы объекты обычно зависят друг от друга. А объекты из разных подсхем друг от друга не зависят.
 * Подсхемы имеют независимые истории изменений.
 *
 * Каждое изменение представлено либо Sql-скриптом, либо Java-скриптом.
 * Каждое изменение имеет идентификатор (имя таска + порядковый номер).
 * Изменения имеют комментарий (могут не иметь, в принципе).
 *
 * Определённое состояние БД после внесения ряда изменений может иметь собственный номер,
 * именуемый версией. Версия представлена строкой составленной таким образом, чтобы
 * давать правильное лексикографическое упорядочение версий при сортировке строк
 * по возрастанию.
 *
 * Если на базе применены миграции, не соответствующие ни одной версии, то версия будет пустой.
 *
 * При мерже модификаций в ветку необходимо использовать согласованную схему именования версий.
 * 1. В каждой ветке, включая транк, версия нумеруется от базовой точке ветвления. К версии
 * точки ветвления прибавляется имя ветки, а дальше порядковый номер версии. Например:
 *
 * Версия в транке была 5.0.01.20 (здесь 20 - число обновлений с последнего вывода ветки).
 * Хотим сделать новую ветку 5.0.02.
 * В транке создаётся новая версия 5.0.02.0, совпадающая с 5.0.01.20.
 * В ветке создаётся новая версия 5.0.02.branch.0, тоже совпадающая с 5.0.01.20.
 * Дальше в транке изменения приводят к увеличению номера на 1 5.0.02.1, 5.0.02.2, 5.0.02.3 ...
 * В ветке изменения, которые сливаются, также приводят к увеличению номера на 1.
 *
 * @author a.zhizhelev
 */
object Migrations extends MigrationsT with ResourceReading
trait MigrationsT {
  private
  object sameThreadExecutionContext extends ExecutionContext {
      override def execute(runnable: Runnable): Unit = runnable.run()
      override def reportFailure(t: Throwable): Unit = throw t
    }

  /** Идентификатор подсхемы. */
  type SubSchemaId = String
  /** Идентификатор объекта в БД */
  type DatabaseObjectId = String

  /** Идентификатор точки на графе зависимостей. Представляет собой URN (universal resource name).
    * Обычно содержит идентификатор версии - 5.0.1.trunk.1
    */
  type DependencyTagUrn = String
  /** Идентификатор обновления. Представляет собой URN (universal resource name).
    * Одновременно служит идентификатором точки на графе зависимостей, которая образуется сразу после
    * выполнения скрипта.
    */
  type MigrationUrn = DependencyTagUrn

  /** Скрипт идентифицируется уникальным URL, составленным из схемы, номера таска и идентификатора скрипта.
    * urn:subschema/taskNumber/scriptNumber */
  def migrationUrn(subschema: SubSchemaId, task: String, migrationNumber: String): MigrationUrn =
    s"urn:$subschema/$task/$migrationNumber"

  def versionTagUrn(subschema: SubSchemaId, tag: String): DependencyTagUrn =
    s"urn:$subschema/v/$tag"

  sealed trait Script {
    /** Information to store in the applied migrations table. */
    def text: String
  }

  /**
   * Представление скрипта обновления указанного объекта БД.
   * objectId идентификатор обновляемого объекта
   * change тип изменения, описываемый этим скриптом
   * id:String, objectId:DatabaseObjectId, change:Change,
   * @param sql скрипт
   */
  case class SqlScript(sql: String) extends Script {
    def text = sql
  }

  case object EmptyScript extends Script {
    def text = ""
  }

  //  /**
  //   * Идентификатор объекта в БД.
  //   *
  //   * withData=true: Объекты, содержащие данные. А именно, таблицы.
  //   *  Управление такими объектами требует строго последовательного выполнения всех обновлений.
  //   * withData=false: Объекты, не содержащие данных, Например, функции.
  //   *  Такие объекты заменяются на последнюю имеющуюся версию.
  //   */
  //  case class DatabaseObjectId(schema:String, name:String, objectType:String, withData:Boolean)
  //
  //  class Schema(schema:String) {
  //    /** Идентификатор таблицы БД. */
  //    def tableId(name: String) = DatabaseObjectId(schema, name, "TABLE", withData = true)
  //  }

  /** Интерфейс для Java-скриптов.
    * Выполняет обновление с помощью кода.
    */
  trait CustomMigrationScript extends Script {
    def execute(implicit session: Session)

    def text = this.getClass.getName
  }

  /** Тег, отмечающий некоторую точку на графе зависимостей.
    * Существует две разновидности тега - пустой тег, отмечающий просто
    * узел в графе зависимостей, и тег, содержащий скрипт. В последнем случае идентификатор
    * скрипта также служит идентификатором узла графа, расположенного
    * сразу после скрипта.
    * */
  trait DependencyNodeTag {
    /** идентификатор определённой точки на графе зависимостей, представляющей собой совокупность скриптов обновления.
      */
    def tagUrn: DependencyTagUrn

    def comment: String

    def dependencies: Seq[DependencyNodeTag] = Seq()

    lazy val dependencySet = dependencies.toSet

    def content: String
  }

  /** идентификатор определённой версии БД, представляющей собой совокупность скриптов обновления.
    * Можно использовать номер версии программы или версию транка.
    * Желательно, чтобы tag был коротким, чтобы можно было использовать в имени шаблона БД.
    *
    * Версия может не иметь названия. В db.properties записывается версия в том случае, если
    * состав обновлений в точности соответствует некоторой объявленной версии БД.
    *
    * Объявляется версия как базовая плюс несколько обновлений.
    * val version419 = version(version418, migrationAllods544613)
    */

  trait VersionTag extends DependencyNodeTag {
    def subSchemaId: SubSchemaId

    def version: String

    def tagUrn = versionTagUrn(subSchemaId, version)

    def content = ""
  }

  /** Миграция БД. */
  trait MigrationDefinition extends DependencyNodeTag {
    /** идентификатор скрипта. Можно использовать номер таска и номер скрипта в пределах таска. */
    def migrationUrn: MigrationUrn

    override def tagUrn = migrationUrn

    def script: Script

    def content = script.text
  }

  /**
   * Менеджер обновлений БД хранит в БД полный список обновлений
   **/
  trait DbMigrationManager {
    /** Все обновления БД, собранные в линейную цепочку.
      * При необходимости следует предварительно вызвать метод Migrations.migrationsToSeq,
      * который построит такую цепочку по графу зависимостей. */
    val migrationPack: MigrationPack
    /** Имя схемы, которая используется для хранения данных обновлений.
      * В этой схеме автоматически будут созданы стандартные таблицы, позволяющие */
    val managementSchemaName: String

    /** Отрисовывает список миграций, которые будут выполнены на пустой БД. */
    def migrationsFullCreatePrettyPrint =
      migrationPack.dependencySeq.map(m =>
        "-- " + m.tagUrn + "\n" +
          "-- " + m.comment + "\n" +
          "-- DEPENDS ON: " + m.dependencies.map(_.tagUrn).mkString(", ") + "\n" +
          m.content +(if(m.content.endsWith(";")||m.content.endsWith(";\n")) "\n" else ";\n")).
        mkString("\n")

    def loadDbio: DBIO[Seq[AppliedMigration]]

    def findRequiredMigrationsDbio: DBIO[SeqMigration]

    def upgradeDbio: DBIO[Seq[AppliedMigration]]

    def isMigrationRequiredDbio(implicit ec: ExecutionContext): DBIO[Boolean] =
      findRequiredMigrationsDbio.map(_.nonEmpty)

    def findUnknownMigrationsDbio: DBIO[SeqAppliedMigration]

    def migrationsDiffPrettyPrintDbio(implicit ec: ExecutionContext): DBIO[String] =
      for {req <- findRequiredMigrationsDbio
           unk <- findUnknownMigrationsDbio
      } yield {
        "\n" +
          req.map(m => "+ " + m.tagUrn + " " + m.comment).mkString("\n") +
          unk.map(m => "- " + m.tagUrn + " " + m.comment).mkString("\n")
      }

    def migrationsDiffDbio(implicit ec: ExecutionContext): DBIO[String] =
          for {req <- findRequiredMigrationsDbio
               unk <- findUnknownMigrationsDbio
          } yield
            (req.map("+" + _.tagUrn) ++
              unk.map("-" + _.tagUrn)).mkString(",")

    def ensureMigrationSupportDbio:DBIO[Unit]
    def versionOptDbio: DBIO[Option[DependencyTagUrn]]

  }

  /** JavaAPI
    * Преобразует Java DataSource в Slick Database. */
  def dataSourceToDatabase(dataSource: DataSource): Database = Database.forDataSource(dataSource)

  /** Применённое изменение в БД. */
  case class AppliedMigration(migrationUrn: String, startTime: Timestamp, endTime: Timestamp, script: String, comment: String) {
    def tagUrn = migrationUrn
  }

  val AppliedMigrationsTableName = "applied_migrations"
  val MigrationsSchema = Some("migrations")

  /** Представление обновлений в БД.
    * Дополнительно содержит время запуска обновления,
    * время окончания обновления, журнал ошибок обновления.
    */
  class AppliedMigrations(tag: Tag) extends Table[AppliedMigration](tag, MigrationsSchema, AppliedMigrationsTableName) {
    //    def migrationId = column[Long]("migrationId", O.PrimaryKey, O.AutoInc)

    def migrationUrn = column[String]("migration_urn", O.PrimaryKey, O.SqlType("text"))

    def migrationStart = column[Timestamp]("migration_start")

    def migrationEnd = column[Timestamp]("migration_end")

    // для Java-скрипта делается toString
    def script = column[String]("script", O.SqlType("text"))

    def comment = column[String]("comment", O.SqlType("text"))

    def * = (migrationUrn, migrationStart, migrationEnd, script, comment) <>(AppliedMigration.tupled, AppliedMigration.unapply)

  }

  val appliedMigrations = TableQuery[AppliedMigrations]

  /** Версия БД хранится в отдельной таблице. В случае, если набор миграций не соответствует
    * никакой версии, таблица будет пустой. */
  class DbVersion(tag: Tag) extends Table[(String, String)](tag, MigrationsSchema, "db_version") {
    def schema = column[String]("schema", O.PrimaryKey, O.SqlType("text"))

    def version = column[String]("version", O.SqlType("text"))

    def * = (schema, version)
  }

  val dbVersion = TableQuery[DbVersion]

  val schemaDdl: DDL = DDL(
    List(s"CREATE SCHEMA IF NOT EXISTS ${MigrationsSchema.get}"), List(),
    List(s"DROP SCHEMA IF EXISTS ${MigrationsSchema.get}"), List())

  val migrationsDbDdl =
    schemaDdl ++
      appliedMigrations.schema ++
      dbVersion.schema

  // TODO: журнал выполнения хранится в отдельной таблице. use JdbcAppender

  /**
   * Реализация топологической сортировки.
   * При обнаружении циклических зависимостей генерируется исключение.
   *
   * @see https://gist.github.com/ThiporKong/4399695
   * @see http://en.wikipedia.org/wiki/Topological_sorting
   * @param elements коллекция всех элементов, которые требуется упорядочить.
   * @tparam T тип элементов. Для него должна быть реализация PartialOrdering'а
   * @return отсортированные по зависимостям элементы.
   */
  def topologicalSort[T](elements: TraversableOnce[T], directDependencies: T => Set[T]): Seq[T] = {
    @tailrec
    def tsort(elementsWithPredecessors: Seq[(T, Set[T])], alreadySorted: ListBuffer[T]): Seq[T] = {
      val (noPredecessors, hasPredecessors) =
        elementsWithPredecessors.partition(_._2.isEmpty)

      if (noPredecessors.isEmpty) {
        if (hasPredecessors.isEmpty)
          alreadySorted.toSeq
        else
          throw new IllegalStateException(
            "There seems to be a cycle in the dependency subgraph: " + elementsWithPredecessors.map(_._1).mkString(","))
      } else {
        val found = noPredecessors.map(_._1)
        val restWithoutFound = hasPredecessors.map(p => (p._1, p._2 -- found)) //оставшиеся миграции с удалёнными узлами.
        tsort(restWithoutFound, alreadySorted ++ found)
      }
    }

    val elementsWithPredecessors = elements.map(e => (e, directDependencies(e))).toSeq
    val keys = elements.toSet
    require(elementsWithPredecessors.forall(_._2.forall(keys.contains)),
      "It is required that all dependent elements appear in elements argument. The following element is absent:"+
        elementsWithPredecessors.find(p => !p._2.forall(keys.contains))
    )
    tsort(elementsWithPredecessors, ListBuffer())
  }

  /** Собирает все зависимости. 
    * Сохраняет порядок их объявления.
    * @param targets несколько зависимостей верхнего уровня
    * @return все миграции, собранные транзитивно.
    */
  def collectAll(targets: List[DependencyNodeTag]): Seq[DependencyNodeTag] = {
    def collectAll0(targets: List[DependencyNodeTag], urns: Set[DependencyTagUrn], result: mutable.ListBuffer[DependencyNodeTag]): Seq[DependencyNodeTag] = targets match {
      case Nil =>
        result.toSeq
      case head :: tail =>
        if (urns.contains(head.tagUrn)) // игнорируем собранные ранее объекты
          collectAll0(tail, urns, result)
        else {
          result += head
          collectAll0(head.dependencies.toList ::: tail, urns + head.tagUrn, result)
        }
    }
    collectAll0(targets, Set(), mutable.ListBuffer())
  }

  /** Вычисляет цепочку миграций, которые зависят от указанных, включая указанные. Порядок
    * миграций строго определён, исходя из обхода графа.
    *
    * Сначала рекурсивным обходом мы собираем все Migration'ы. При этом зависимости могут быть
    * отсортированы некорректно. Затем мы просто сортируем зависимости по транзитивному отношению hasDependencyOn
    *
    * При обнаружении циклических зависимостей генерируется исключение.
    * */
  def collectAndSortDependencies(targets: List[DependencyNodeTag]): Seq[DependencyNodeTag] = {
    val collected = collectAll(targets)

    val urns = collected.map(_.tagUrn)
    val duplicatedUrns = urns.groupBy(identity).map(p => (p._1, p._2.size)).filter(_._2>1)
    require(duplicatedUrns.isEmpty, "There are duplicates in the list of migrations: "+duplicatedUrns.mkString(","))

    val map = collected.map(n => n.tagUrn -> n).toMap
    val graph = map.map{case (name, node) => (name, node.dependencies.map(_.tagUrn).toSet)}

    val sortedUrns = topologicalSort[DependencyTagUrn](urns, graph)
    sortedUrns.map(map)
  }

  /** Последовательность скриптов обновления. Все скрипты собраны в линейную цепочку.
    * Прогон всей цепочки должен быть валидной операцией. */
  type SeqMigration = Seq[DependencyNodeTag]

  case class MigrationPack(dependencySeq: Seq[DependencyNodeTag])
  {
    def versionOpt = dependencySeq.reverse.collect { case v: VersionTag => v }.headOption
    def migrations: Seq[MigrationDefinition] = dependencySeq.collect { case m: MigrationDefinition => m}
  }

  /** Формирует MigrationPack для линейной цепочки зависимостей.
    * Если на самом верху находится версия, то MigrationPack будет иметь версию.
    * В противном случае, версия будет отсутствовать. */
  def dependencySeqToMigrationPack(dependencySeq: Seq[DependencyNodeTag]) =
    MigrationPack(dependencySeq)

  def createMigrationPack(targets: DependencyNodeTag*): MigrationPack = {
    val sorted = collectAndSortDependencies(targets.toList)

    dependencySeqToMigrationPack(sorted)
  }


  /**
   * Совокупность применённых обновлений
   */
  type SeqAppliedMigration = Seq[AppliedMigration]

  @deprecated("use isMigrationsMetaExistsDbio", "23.06.2015")
  def isMigrationsMetaExists(implicit session: Session) =
    dbRunAwait(isMigrationsMetaExistsDbio(sameThreadExecutionContext))

  private def dbRunAwait[T](action: DBIO[T])(implicit session: PostgresDriver.api.Session): T =
    scala.concurrent.Await.result(session.database.run(action), scala.concurrent.duration.Duration.Inf)

  /** Проверяет наличие метаданных по миграциям и создаёт таблицы в случае их отсутствия. */
  @deprecated("use ensureMigrationSupportDbio", "23.06.2015")
  def ensureMigrationSupport(implicit session: Session): Unit =
    dbRunAwait(ensureMigrationSupportDbio(sameThreadExecutionContext))

  def isMigrationsMetaExistsDbio(implicit ec: ExecutionContext): DBIO[Boolean] = MTable.getTables(
    cat = None,
    schemaPattern = MigrationsSchema,
    namePattern = Some(AppliedMigrationsTableName),
    types = None
  ).map(_.nonEmpty)

  def ensureMigrationSupportDbio(implicit ec: ExecutionContext): DBIO[Unit] =
    isMigrationsMetaExistsDbio.flatMap {
      t => if(t) DBIO.successful(()) else migrationsDbDdl.create
    }.transactionally

  // https://www.safaribooksonline.com/library/view/regular-expressions-cookbook/9781449327453/ch08s06.html
  val urnRegex = "^urn:[a-z0-9][a-z0-9-]{0,31}:[a-z0-9()+,\\-.:=@;$_!*'%/?#]+$".r
  def isUrnValid(urn:String):Boolean =
    urnRegex.findFirstIn(urn).nonEmpty

}

