import org.apache.flink.streaming.api.scala.{DataStream,StreamExecutionEnvironment,_}
import org.apache.flink.streaming.api.windowing.time.Time

object SocketStream {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999, '\n')

    val windowCounts = text.flatMap(_.split(" ")).map(x=>WordWithCount(x,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
    windowCounts.sum("count").print()
    env.execute("socket demo")
  }

  case class WordWithCount(word: String, count: Long)
}
