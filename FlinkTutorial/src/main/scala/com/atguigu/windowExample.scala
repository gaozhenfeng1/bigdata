package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object windowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val Dstream: DataStream[(String, Double)] = env.addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))
    Dstream.print()
    env.execute()





  }
}
