package allods.db.migration

import java.io.{InputStreamReader, Reader}
import scala.util.parsing.combinator.RegexParsers
import scala.language.postfixOps
import scala.util.parsing.input.Position
import java.net.URL

/**
 * Splits an sql script into independent statements.
 *
 * The statement is considered finished when a semicolon is encountered.
 *
 * There are a few escapes for semicolons:
 *
 * 1. "..."
 * 2. '...'
 * 3. $something$ ... $something$, where $something$ in the beginning and at the end are the same.
 * 4. -- ... \n
 *
 * // it would be possible to import JdbcTestUtils.executeSqlScript or  iBatis, however, the proposed splitter
 * has an advantage - it supports ordinary postrgreSql-scripts without additional artificial delimiters.
 * @author a.zhizhelev
 */
object PostgreSqlScriptSplitter {

  /** Statement with position info. */
  case class Statement(sql: String, pos: Position)

  private
  object P extends RegexParsers {

    override val skipWhitespace = false

    /** one line comment */
    def comment = "--.*$".r

    /** Any text before quotes. */
    def ORDINARY_CHARS = """[^$"';-]+""".r

    def quotedString = '\"' ~> "[^\"]".r <~ '\"' ^^ ("\"" + _ + '\"')

    def squotedString = '\'' ~> "[^\']".r <~ '\'' ^^ ("\'" + _ + '\'')

    def dollarStringStartTag = ("$" ~> "[^$]*".r <~ "$") map ("$" + _ + "$")

    def dollarString: Parser[String] = dollarStringStartTag.flatMap {
      tag =>
        (rep("[^$]+".r | not(tag) ~> "$") <~ tag).
          map(tag + _.mkString("") + tag)
    }

    def statementPiece: Parser[String] =
      ORDINARY_CHARS |
      dollarString |
        quotedString | squotedString | comment |
        "\"" | "\'" | "$" | "-"

    def statementString =
      rep(statementPiece) map (_.mkString(""))

    /** We want to keep the original position of the statement in the script file. */
    def statement: Parser[Statement] = new Parser[Statement] {
      def apply(input: Input) = {
        val startPos = input.pos
        statementString(input).map(Statement(_, startPos))
      }
    }

    def statements = repsep(statement, ';') // <~ (not(statement) ~> anyChar *)
  }

  def apply(script: String): Seq[Statement] =
    P.parseAll(P.statements, script).get

  def apply(reader: Reader): Seq[Statement] =
    P.parseAll(P.statements, reader).get

  def apply(url: URL): Seq[Statement] = {
    if (url == null)
      throw new NullPointerException("Url is null")
    val r = new InputStreamReader(url.openStream(), "utf8")
    try {
      apply(r)
    } finally {
      r.close()
    }
  }
}
