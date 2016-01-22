package allods.db.migration

/**
 * Test db server description.
 *
 * DB-server address can be adjusted by environment properties:
 *
 * -Dtest.database.url=jdbc:postgresql://dev-db.a1.pvt:5432/  -Dtest.database.login=allods_online_t
 *
 * @author a.zhizhelev
 */
object TestDbServer {
  val urlProperty = "test.database.url"
  val loginProperty = "test.database.login"
  val passwordProperty = "test.database.password"

  val defaultDbHostSettings = Map(
    urlProperty -> "jdbc:postgresql://localhost:5432/",
    loginProperty -> "allods_online_t",
    passwordProperty -> ""
  )

  //  val testDbServer = new DbServerPointer("jdbc:postgresql://dev-db.a1.pvt:5432/", "allods_online_t", "")
  //  val (devDbServer, user, password) = ("jdbc:postgresql://dev-db.a1.pvt:5432/", "allods_online_t", "")
  //  val devDbServer = "jdbc:postgresql://localhost:5432/"; val user = "postgres";  val password = "postgres"
  //  val testDbName = "test"

  lazy val envDbHostSettings = defaultDbHostSettings.map { case (prop, default) =>
    val propValue = System.getProperty(prop, default)
    val propValueOrDefault = if (propValue.isEmpty) default else propValue
    (prop, propValueOrDefault)
  } //sys.env.getOrElse(prop, default))}

  lazy val testDbServer = new DbServerPointer()(envDbHostSettings(urlProperty), envDbHostSettings(loginProperty), envDbHostSettings(passwordProperty))

}
