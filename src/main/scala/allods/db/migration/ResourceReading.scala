package allods.db.migration

import java.net.URL

/** A conveniency trait for reading resources.
 * @author a.zhizhelev
 */
trait ResourceReading {
  /** Reads resource for the given class. */
  def readResource(resourcePath: String, enc: String = "utf8") =
    ResourceReading.readResource(getClass, resourcePath, enc)
}

object ResourceReading {
  /** Reads resource for the given class. */
  def readResource(clazz: Class[_], resourcePath: String, enc: String = "utf8") = {
    val resource: URL = clazz.getResource(resourcePath)
    if (resource == null)
      throw new IllegalArgumentException(s"Resource not found: $resourcePath in class ${clazz.getName}")
    io.Source.fromURL(resource, enc).mkString
  }

}