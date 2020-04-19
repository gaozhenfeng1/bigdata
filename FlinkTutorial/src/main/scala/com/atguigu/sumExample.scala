package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object sumExample {
  def main(args: Array[String]): Unit = {
    //统计每个窗口的温度有多少条数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[(String, Int)] = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new MySum)
    ds.print()
    env.execute()
  }

   class MySum extends AggregateFunction[SensorReading,(String,Int),(String,Int)] {
     override def createAccumulator(): (String, Int) = ("",0)

     override def add(value: SensorReading, accumulator: (String, Int)): (String, Int) =
       (value.id,accumulator._2+1)

     override def getResult(accumulator: (String, Int)): (String, Int) =accumulator

     override def merge(a: (String, Int), b: (String, Int)): (String, Int) =
       (a._1,a._2+b._2)
   }

}