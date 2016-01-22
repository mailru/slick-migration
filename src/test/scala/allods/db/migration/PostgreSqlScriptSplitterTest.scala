package allods.db.migration

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


/**
 * @author a.zhizhelev
 */
@RunWith(classOf[JUnitRunner])
class PostgreSqlScriptSplitterTest extends FunSuite {
  test("Parse simple script") {
    val sql = "CREATE TABLE"
    val statements = PostgreSqlScriptSplitter(sql)
    // pattern matching to check that we have parsed properly.
    val Seq(PostgreSqlScriptSplitter.Statement(`sql`, _)) = statements
  }

  test("Parse script with quotes") {
    val sql = "CREATE TABLE 'name''ав\"'"
    val statements = PostgreSqlScriptSplitter(sql)
    // pattern matching to check that we have parsed properly.
    val Seq(PostgreSqlScriptSplitter.Statement(`sql`, _)) = statements
  }

  test("Parse resource script") {
    val testSql = classOf[PostgreSqlScriptSplitterTest].getResource("test.sql")
    val statements = PostgreSqlScriptSplitter(testSql)
    assert(statements.size === 8)
  }

  //  test("Parse simple script"){
  //    val result = PostgreSqlScriptSplitter("$123$CREATE TABLE;$123$")
  //    assert(result === "$123$CREATE TABLE;$123$")
  //  }
}
