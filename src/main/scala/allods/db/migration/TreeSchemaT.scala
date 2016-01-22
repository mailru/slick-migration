package allods.db.migration

/** Представление структуры БД в виде дерева, удобного для сравнения.
  * Структура отражает только существенную часть схемы БД, отличия в которой должны быть ликвидированы миграциями.
  *
  * Включает схемы, таблицы, колонки таблиц, функции, тела функций.
  */
trait TreeSchemaT {
  type NodeKind = String

  //  sealed trait NodeKind
  //  case object RootNodeKind extends NodeKind
  //  case object SchemeNodeKind extends NodeKind
  //  case object TableNodeKind extends NodeKind
  //  case object ColumnNodeKind extends NodeKind
  //  case object FunctionNodeKind extends NodeKind

  /** Основа дерева - именованный узел. */
  sealed trait Node

  /** Родительский узел, имеющий потомков.
    * Потомки доступны по именам.
    * Структура имён включает типы узлов. В частности, schema, table, column, function, trigger, etc. */
  case class ParentNode(children: Map[String, Node]) extends Node

  /** Листовой узел, имеющий конкретное наполнение. */
  case class ContentNode(content: Any) extends Node

  /** Просматривает два дерева на предмет различий.
    * На одном уровне сравниваются узлы, имеющие одинаковые наименования и контент.
    * @param left условное имя левого дерева. Либо Full schema, либо Migrations schema
    * @param right условное имя правого дерева. Либо Full schema, либо Migrations schema
    */
  def difference(left: String, right: String, n1: Node, n2: Node): Stream[String] = {
    def difference0(path: String, n1: Node, n2: Node): Stream[String] = (n1, n2) match {
      case (ContentNode(content1), ContentNode(content2)) =>
        if (content1 == content2)
          Stream.empty
        else
          Stream(s"@$path: content (!=):\n$left:\n\t$content1\n$right:\n\t$content2")
      case (ParentNode(children1), ParentNode(children2)) =>
        val s1 = children1.keySet
        val s2 = children2.keySet
        val s12 = s1 -- s2
        val s21 = s2 -- s1
        val s = s1.intersect(s2)
        (if (s12.isEmpty)
          Stream.empty[String]
        else
          Stream(s"@$path: $left -- $right = ${s12.toSeq.sorted.mkString(",")}")
          ).append(
            if (s21.isEmpty)
              Stream.empty[String]
            else
              Stream(s"@$path: $right -- $left = ${s21.toSeq.sorted.mkString(",")}")
          ).append(
            if (s.isEmpty)
              Stream.empty[String]
            else
              s.toSeq.sorted.toStream.flatMap(key => difference0(path + " " + key, children1(key), children2(key)))
          )
      case _ =>
        Stream(s"@$path: Cannot find difference of @$path/${n1.getClass}, @$path/${n2.getClass}")
    }
    difference0("", n1, n2)
  }
}
