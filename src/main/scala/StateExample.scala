import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object StateExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream:DataStream[(Int,Long)] = env.fromElements((1,21L),(2,4L),(3,52L))
    inputStream.keyBy(_._1).flatMap(new RichFlatMapFunction[(Int,Long),(Int,Long,Long)] {
      private var leastValueState:ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        val leastValueStateDescriptor = new ValueStateDescriptor[Long]("leastValue",classOf[Long])
        leastValueState = getRuntimeContext.getState(leastValueStateDescriptor)
      }

      override def flatMap(in: (Int, Long), out: Collector[(Int, Long, Long)]): Unit = {
        val leastValue = leastValueState.value()
        if(in._2>leastValue){
          out.collect((in._1,in._2,leastValue))
        }else{
          leastValueState.update(in._2)
          out.collect((in._1,in._2,in._2))
        }
      }
    }).print()

    env.execute()
  }
}
