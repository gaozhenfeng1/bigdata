package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object sideOutputExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource).process(new FreezingMonitor)
//    stream.print()
    stream.getSideOutput(new OutputTag[String]("freezing")).print()
    env.execute()
  }

  class FreezingMonitor extends ProcessFunction[SensorReading,SensorReading]{
    lazy val freezingAlermOutput = new OutputTag[String]("freezing")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature<35.0){
        ctx.output(freezingAlermOutput,s"传感器${value.id}低温预警")
      }
      out.collect(value)
    }
  }
}