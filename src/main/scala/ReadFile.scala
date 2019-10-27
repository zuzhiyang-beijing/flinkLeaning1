import scala.io.Source

object ReadFile {

  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("/workspace/flinkLeaning/src/main/scala/wordFile")
    val lines = file.getLines()
    for (content <- lines)
      println(content)
  }
}
