package allods.db.migration

import javax.sql.DataSource

import allods.db.migration.Migrations._

case class VersionTagImpl(subSchemaId: String, version: String, comment: String = "", override val
dependencies: Seq[DependencyNodeTag] = Seq()) extends VersionTag {
  def comment(comment: String) =
    copy(comment = comment)

  def dependsOn(deps: DependencyNodeTag*) =
    copy(dependencies = dependencies ++ deps)
}

/** Обновление БД.
  * Имеет идентификатор и скрипт, который выполняет обновление.
  *
  * Также содержит DSL, который позволяет последовательно сконструировать описание обновления.
  */
case class MigrationDefinitionImpl
(migrationUrn: MigrationUrn,//TODO: validate urn
 comment: String = "", script: Script = EmptyScript,
 override val
 dependencies: Seq[DependencyNodeTag] = Seq()) extends MigrationDefinition {

  def comment(comment: String): MigrationDefinitionImpl =
    copy(comment = comment)

  def script(script: Script) =
    copy(script = script)

  def sql(sql: String) =
    copy(script = SqlScript(sql))

  def sqlResource(clazz: Class[_], resourcePath: String, enc: String = "utf8") =
    sql(ResourceReading.readResource(clazz, resourcePath, enc))

  def dependsOn(deps: DependencyNodeTag*) =
    copy(dependencies = dependencies ++ deps)

  def andThen(migration: MigrationDefinitionImpl) =
    migration.dependsOn(this)
}

/** Предок для пользовательских описаний обновлений.
  * В этом классе описан dsl, позволяющий пользователю описывать структуру обновлений
  * с помощью скриптов.
  * @author a.zhizhelev
  */
abstract class MigrationBuilder(val subschema: SubSchemaId) extends ResourceReading {

  def migrate(task: String, scriptId: String = "0") = MigrationDefinitionImpl(migrationUrn(subschema, task, scriptId))

  def version(v: String) = VersionTagImpl(subschema, v)

  def createMigrationManager(migrations: DependencyNodeTag*): DbMigrationManager =
    new DbMigrationManagerImplSessionApi(subschema, createMigrationPack(migrations: _*))

  def createMigrationManager(migrationPack: MigrationPack): DbMigrationManager =
    new DbMigrationManagerImplSessionApi(subschema, migrationPack)

  import slick.driver.PostgresDriver.api._

  def dataSourceToSlickDatabase(dataSource: DataSource): Database =
    Database.forDataSource(dataSource)

  def readResourceUtf8(resourcePath: String) =
    readResource(resourcePath, "utf8")

}