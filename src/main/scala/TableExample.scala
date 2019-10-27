import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._

object TableExample{

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tstreamEnv = StreamTableEnvironment.create(env)
    val stream:DataStream[(Int,String)] = env.fromElements((1,"zhangsan"),(2,"lisi"))
    tstreamEnv.registerDataStream("testTable",stream,'id,'name)
    val projTable = tstreamEnv.scan("testTable").select("*")
    env.execute()
  }
}
