package allods.db.migration

import java.util.concurrent.TimeUnit

import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend._
import slick.jdbc.StaticQuery

/**
 * Points to a Postgres server.
 *
 * @param dbServer сервер БД вместе с jdbc-префиксом. Вида: "jdbc:postgresql://127.0.0.1:5432/"
 *                 Пока поддерживается только Postgres
 * @param user имя пользователя, под которым надо подключаться к БД
 * @param password пароль пользователя
 *
 * @author a.zhizhelev
 */
class DbServerPointer(val driver: JdbcDriver = slick.driver.PostgresDriver)(val dbServer: String, val user: String, val password: String) {

  require(!dbServer.isEmpty)

  val postgresDriver = classOf[org.postgresql.Driver].getName

  def connect(db: String): Database =
    Database.forURL(dbServer + db, user, password, driver = postgresDriver)

  def withTemporaryDb[T](nameHint: String)(body: (Session) => T): T =
    temporaryDb(nameHint)(_.withSession(body))

  def temporaryDb[T](nameHint: String)(body: (Database) => T): T = {
    val connect1: Database = connect("postgres")
    val name = nameHint + "_testdb_" + System.currentTimeMillis
    connect1.withSession(
      createSession => StaticQuery.updateNA("create database \"" + name + "\" with template template1;").execute(createSession)
    )

    try {
      body(connect(name))
    } finally {
      connect1.withSession(
        dropSession => StaticQuery.updateNA("drop database \"" + name + "\";").execute(dropSession)
      )
    }
  }

  def measureMs(body: => Any): Long = {
    val startNs = System.nanoTime()
    body
    val deltaNs = System.nanoTime() - startNs
    TimeUnit.NANOSECONDS.toMillis(deltaNs)
  }

  override
  def toString = getClass.getSimpleName + "(" +
    "dbServer=" + dbServer +
    ", login=" + user +
    ", password=" + password.map(c => '*') +
    ")"
}
