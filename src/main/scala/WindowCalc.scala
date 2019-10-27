import java.util.stream.Collector

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}

object WindowCalc {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream:DataStream[(String,Long)] = env.fromElements(("aa",1),("bb",2))
    val result = inputStream.keyBy(_._1).timeWindow(Time.seconds(10))
      /*.reduce((v1:(String,Long),v2:(String,Long))=>{
        if (v1._2>v2._2) v2 else v1
      },
        (key:String,
          window:TimeWindow,
        minReadings:Iterable[(String,Long,Int)],
        out:Collector[(Long,(String,Long,Int))])=>{
      val min = minReadings.iterator.next()
          out.collect(window.getEnd,min)
      })*/
  }

  def myreduceFunction(env :StreamExecutionEnvironment): Unit ={
    val inputStream :DataStream[(Int,Long)] = env.fromElements((1,1l),(2,2l))
    val reduceWindowStream = inputStream.keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(10)))
      .reduce((v1,v2)=>(v1._1,v1._2+v2._2))
  }
}
