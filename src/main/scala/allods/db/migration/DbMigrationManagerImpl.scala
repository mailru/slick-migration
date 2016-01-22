package allods.db.migration

import java.sql.Timestamp

import allods.db.migration.Migrations._
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * Migration manager API implementation.
  *
  * @param managementSchemaName the db-schema name for system tables (applied_migrations, db_version)
  * @param migrationPack the collection of migrations declared in the application.
  * @author a.zhizhelev
  */
class DbMigrationManagerImpl
(val managementSchemaName: String,
 val migrationPack: MigrationPack) extends Migrations.DbMigrationManager {
  val migrationsOrdered = {
    migrationPack.dependencySeq sortWith {
      case (left, right) => right.dependencySet contains left
    }
  }

  def ensureMigrationSupportDbio: DBIO[Unit] =
    Migrations.ensureMigrationSupportDbio

  def upgradeDbio: DBIO[Seq[AppliedMigration]] =
    findRequiredMigrationsDbio.flatMap { found =>
      found.map(applyMigrationDbio).
        foldLeft(DBIO.successful(Seq()): DBIO[Seq[AppliedMigration]]) {
        case (prev, next) =>
          prev.flatMap { case seq => next.map(a => seq ++ a) }
      }
    }.withPinnedSession


  def findRequiredMigrationsDbio: DBIO[SeqMigration] = {
    appliedMigrations.result.map { loaded =>
      val appliedSet = loaded.map(_.migrationUrn).toSet
      migrationPack.dependencySeq.filterNot(u => appliedSet.contains(u.tagUrn))
    }
  }

  def findUnknownMigrationsDbio: DBIO[SeqAppliedMigration] = {
    val migrationIds = migrationsOrdered.map(_.tagUrn).toSet
    loadDbio.map(_.filterNot(am => migrationIds.contains(am.migrationUrn)))
  }

  def loadDbio: DBIO[SeqAppliedMigration] =
    appliedMigrations.result

  def now = System.currentTimeMillis()

  def applyMigrationDbio(migration: DependencyNodeTag): DBIO[Seq[AppliedMigration]] = migration match {
    case m: MigrationDefinition =>
      import m._
      val dbioAction = script match {
        case SqlScript(sqlScript) =>
          val statements = PostgreSqlScriptSplitter(sqlScript)
          statements.foldLeft(DBIO.successful((now, now)): DBIO[(Long, Long)]) {
            case (prev, statement) =>
            prev.flatMap { case (start, _) =>
              statement match {
                case PostgreSqlScriptSplitter.Statement(sql, pos) =>
                  SimpleDBIO[(Long, Long)] { ctx =>
//                    val start = now
                    val t = Try {
                      ctx.session.prepareStatement(sql).execute()
                    }
                    if (t.isFailure)
                      throw new IllegalStateException(s"[Migration] Migration $migrationUrn failed. Couldn't execute statement in script at position $pos. SQL:\n$sql", t.failed.get)
                    (start, now)
                  }.asInstanceOf[DBIO[(Long, Long)]]
              }
            }
          }
        case j: CustomMigrationScript =>
          SimpleDBIO[(Long, Long)] { ctx => val start = now; j.execute(ctx.session); (start, now) }
        case EmptyScript =>
          SimpleDBIO[(Long, Long)] { ctx => (now, now) }
      }
      // id = -1: slick for inserts ignores ids for AutoInc fields.
      dbioAction.flatMap { case (start, end) =>
        appliedMigrationInsert(AppliedMigration(migrationUrn, new Timestamp(start), new Timestamp(end), content, comment))
      }.transactionally
    case version: VersionTag =>
      dbVersion.filter(_.schema === version.subSchemaId).delete.andThen(
        dbVersion.forceInsert(version.subSchemaId, version.version)).andThen(
          appliedMigrationInsert(AppliedMigration(version.tagUrn, new Timestamp(now), new Timestamp(now), version.content, version.comment))
         //DBIO.successful(Seq()) //AppliedMigration(version.tagUrn, new Timestamp(0), new Timestamp(0), version.content, version.comment)))
        ).transactionally

  }

  def appliedMigrationInsert(am: AppliedMigration): DBIO[Seq[AppliedMigration]] =
    appliedMigrations.forceInsert(am).andThen(
      appliedMigrations.filter(_.migrationUrn === am.tagUrn).result
    ).transactionally

  def versionOptDbio: DBIO[Option[DependencyTagUrn]] =
    dbVersion.filter(_.schema === managementSchemaName).map(_.version).result.headOption

}
