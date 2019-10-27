import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment//创建执行环境
    val text = env.readTextFile("file:///workspace/flinkLeaning/src/main/scala/wordFile")//加载数据源
    val counts:DataStream[(String,Int)] = text.flatMap(_.toLowerCase.split(" "))//数据转换操作
      .filter(_!= null).map((_,1)).keyBy(0).sum(1)
    val reduceSource = env.fromElements(("a",3),("b",2),("c",1),("c",3),("a",1))
    reduceSource.keyBy(0).reduce((t1,t2)=>(t1._1,t1._2+t2._2)).print()
    println("Printing result to stdout. Use --output to specify output path.")
    counts.print()//指定输出位置

    /*val dataSet = env.fromElements(("hello",1),("flink",3))
    val groupDataSet:GroupedDataSet[(String,Int)] = dataSet.groupBy(0)
    val result = groupDataSet.max(1)
    result.print()*/

    val personDataSet = env.fromElements(("hello",1),("flink",3))
    val result = personDataSet.keyBy("_1").max("_2").print()


    println("---------custom class----------")
    var persionStream = env.fromElements(new Persion2("peter",14),new Persion2("zu",34))
    val maxAgePersion = persionStream.keyBy("name").max("age")
    maxAgePersion.writeAsCsv("file:///workspace/flinkLeaning/src/main/scala/persion.csv",WriteMode.OVERWRITE)
    maxAgePersion.writeAsText("file:///workspace/flinkLeaning/src/main/scala/persion.txt")
    env.execute("hello flink")//调用execute触发程序
  }

  case class Persion2(name:String,age:Int)
}
