package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Redirect1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .socketTextStream("192.168.1.102",9999,'\n')
      .map(r =>{
        val arr: Array[String] = r.split(" ")
        (arr(0),arr(1).toLong*1000)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .process(new keyed)
    stream.print()
    stream.getSideOutput(new OutputTag[String]("late")).print()
    env.execute()
  }

  class keyed extends KeyedProcessFunction[String,(String,Long),(String,Long)]{
    val late = new OutputTag[String]("late")

    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      if (value._2 < ctx.timerService().currentWatermark()){
        ctx.output(late,value._2+"迟到了")
      }else{
        out.collect(value)
      }

    }
  }

}