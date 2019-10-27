import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object MyFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream:DataStream[String] = env.fromElements("hello","flink")
    val result = dataStream.map(new ExtendFunction)
    result.print()
    env.execute("mapFunction")
  }
}
