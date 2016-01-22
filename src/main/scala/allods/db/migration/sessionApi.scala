package allods.db.migration

import java.sql.Timestamp

import allods.db.migration.Migrations._
import slick.driver.PostgresDriver
import slick.driver.PostgresDriver.api._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
  * Legacy session API that will be removed in 3.1.0
  * @author a.zhizhelev
  */
@deprecated("use DbMigrationManager", "09.11.2015")
trait DbMigrationManagerSessionApi {

  /** Загружает из БД все скрипты, которые были выполнены на базе. */
  def load(implicit session: Session): SeqAppliedMigration

  /** Возвращает те апдейты, которые надо выполнить, чтобы привести БД к актуальному состоянию. */
  def findRequiredMigrations(implicit session: Session): SeqMigration

  /** Проверяет наличие метаданных по миграциям и создаёт таблицы в случае их отсутствия. */
  def ensureMigrationSupport(implicit session: Session):Unit

  /** Обновляет БД до текущей версии, применяя необходимые обновления. */
  def upgrade(implicit session: Session)

  /** Требуется ли миграция. */
  def isMigrationRequired(implicit session: Session): Boolean = {
    val migrations: SeqMigration = findRequiredMigrations.collect { case m: MigrationDefinition => m}
    migrations.nonEmpty
  }

  def findUnknownMigrations(implicit session: Session): SeqAppliedMigration

  def migrationsDiffPrettyPrint(implicit session: Session) = {
    "\n" +
      findRequiredMigrations.map(m => "+ " + m.tagUrn + " " + m.comment).mkString("\n") +
      findUnknownMigrations.map(m => "- " + m.tagUrn + " " + m.comment).mkString("\n")
  }

  def migrationsDiff(implicit session: Session):String = {
    (findRequiredMigrations.map("+" + _.tagUrn) ++
      findUnknownMigrations.map("-" + _.tagUrn)).mkString(",")
  }

  def versionOpt(implicit session: Session): Option[DependencyTagUrn]

}

/** An adapter for implicit session:Session. */
@deprecated("use DbMigrationManagerImpl", "09.11.2015")
class DbMigrationManagerImplSessionApi(managementSchemaName: String,
  migrationPack: MigrationPack) extends DbMigrationManagerImpl(managementSchemaName, migrationPack) with DbMigrationManagerSessionApi {
  private def dbRunAwait[T](action: DBIO[T])(implicit session: Session): T =
    scala.concurrent.Await.result(session.database.run(action), scala.concurrent.duration.Duration.Inf)

  /** Обновляет БД до текущей версии, применяя необходимые обновления.
    * Если версия отсутствует, то в таблице dbVersion изменения не производятся.
    * @param session подключение к БД
    */
  override def upgrade(implicit session: Session): Unit = //:Seq[AppliedMigration] =
    dbRunAwait(upgradeDbio)

  /** Проверяет наличие метаданных по миграциям и создаёт таблицы в случае их отсутствия. */
  def ensureMigrationSupport(implicit session: Session): Unit =
    Migrations.ensureMigrationSupport

  def versionOpt(implicit session: Session): Option[String] =
    dbRunAwait(versionOptDbio)

  /** Возвращает те апдейты, которые надо выполнить, чтобы привести БД к актуальному состоянию. */
  override def findRequiredMigrations(implicit session: PostgresDriver.api.Session): SeqMigration =
    dbRunAwait(findRequiredMigrationsDbio)

  /** Возвращает уже применённые апдейты, которые неизвестны текущей версии программы. */
  override def findUnknownMigrations(implicit session: Session): SeqAppliedMigration = {
    val migrationIds = migrationsOrdered.map(_.tagUrn).toSet
    load.filterNot(am => migrationIds.contains(am.migrationUrn))
  }

  /** Загружает из БД все скрипты, которые были выполнены на базе. */
  override def load(implicit session: PostgresDriver.api.Session): Seq[AppliedMigration] =
    dbRunAwait(loadDbio)

  /** Выполняет указанное обновление на базе.
    *
    * Следует вызывать только в том случае, если точно установлено,
    * что это обновление надо выполнить.
    */
  def applyMigration(migration: DependencyNodeTag)(implicit session: Session): AppliedMigration =
    dbRunAwait(applyMigrationDbio(migration)).head

}
