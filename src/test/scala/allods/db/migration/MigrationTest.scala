package allods.db.migration

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import slick.driver.PostgresDriver.api._
import allods.db.migration.Migrations._

/**
 * @author a.zhizhelev
 */
@RunWith(classOf[JUnitRunner])
class MigrationTest extends FunSuite {

  private def dbExecute[T](action:DBIO[T])(implicit session: Session):T =
    scala.concurrent.Await.result(session.database.run(action), scala.concurrent.duration.Duration.Inf)

  /** Базовый класс для деталей схемы БД.
    *
    * Части схемы, относящиеся к одному объекту, описываются в trait'ах.
    * trait'ы могут располагаться в нескольких файлах.
    *
    * */
  abstract class MySchemaBuilder extends MigrationBuilder("test")

  trait MySchema1 extends MySchemaBuilder {
    val table1Creation = migrate("ALLODS-432535")
      .comment("создание таблицы table1 (id)")
      .sql("CREATE TABLE \"table1\" (id bigint)")
    val table1WithName = migrate("ALLODS-432536")
      .dependsOn(table1Creation)
      .comment("добавление в таблицу table1 колонки name")
      .sql("ALTER TABLE \"table1\" ADD COLUMN name text")
    val version1 = version("version1").dependsOn(table1WithName)
  }

  trait MySchema2 extends MySchemaBuilder with MySchema1 {
    // self:SubschemaDefinition =>
    val table2Creation = migrate("ALLODS-432537")
      .comment("создание таблицы table2 (id)")
      .sql("CREATE TABLE table2 (id bigint)")
    val table2WithName = migrate("ALLODS-432535", "1")
      .dependsOn(table1WithName, table2Creation)
      .comment("добавление в таблицу table2 колонки name, а в таблицу table1 - secondName")
      .sql(
        """ALTER TABLE table2 ADD COLUMN name text;
          |ALTER TABLE "table1" ADD COLUMN secondName text""".stripMargin)
    val version2 = version("version2").dependsOn(version1, table2WithName)
  }

  val Version1 = new MySchema1 {}
  val Version2 = new MySchema1 with MySchema2 {}

  trait MyIllegalSchema3 extends MySchemaBuilder with MySchema2 {
    val table3CreationTwice = migrate("ALLODS-264641").
      comment("Создание таблицы t дважды, чтобы при второй операции произошла ошибка.").
    sql(
      """CREATE TABLE tab1 (id bigint);
        |CREATE TABLE tab1 (id bigint);
      """.stripMargin)
    val version3 = version("version3").dependsOn(version2,table3CreationTwice)
  }
  object Version3Illegal extends MyIllegalSchema3
  test("Migrations construction") {
    TestDbServer.testDbServer.withTemporaryDb("MigrationsTestDb") {
      implicit session =>
        val deltaMs = TestDbServer.testDbServer.measureMs {
          info("1")
          dbExecute(Migrations.migrationsDbDdl.create)
          info("2")
          val mp1 = createMigrationPack(Version1.version1)
          assert(mp1.versionOpt.map(_.version) === Some("version1"))
          val migrationManager = Version1.createMigrationManager(mp1).asInstanceOf[DbMigrationManagerSessionApi]
          migrationManager.upgrade
          assert(migrationManager.versionOpt === mp1.versionOpt.map(_.version))

          info("DB version 1")

          val mm2 = Version2.createMigrationManager(Version2.version2).asInstanceOf[DbMigrationManagerSessionApi]
          assert(mm2.versionOpt === Some("version1"))
          assert(mm2.isMigrationRequired, "Version2.isMigrationRequired")
          mm2.upgrade
          assert(mm2.versionOpt === Some("version2"))
          info("DB version 2")
          assert(!mm2.isMigrationRequired, "!Version2.isMigrationRequired")
        }
        info(s"Time=$deltaMs, ms")
    }
  }
  test("Migration rollback") {
    TestDbServer.testDbServer.withTemporaryDb("MigrationsTestDbRollBack") {
      implicit session =>
        val deltaMs = TestDbServer.testDbServer.measureMs {
          info("1")
          dbExecute(Migrations.migrationsDbDdl.create)
          info("2")
          val mp2 = createMigrationPack(Version3Illegal.version2)
          val mp3 = createMigrationPack(Version3Illegal.version3)
//          assert(mp1.versionOpt.map(_.version) === Some("version1"))
          val migrationManager = Version3Illegal.createMigrationManager(mp3).asInstanceOf[DbMigrationManagerSessionApi]
          val exception = intercept[IllegalStateException](migrationManager.upgrade)
          assert(exception.getMessage.contains("tab1"))
//          assert(migrationManager.versionOpt === mp2.versionOpt.map(_.version))
        }
        info(s"Time=$deltaMs, ms")
    }
  }
}
