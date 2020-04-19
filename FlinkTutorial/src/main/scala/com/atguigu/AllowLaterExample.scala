package com.atguigu

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowLaterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .socketTextStream("192.168.1.102",9999,'\n')
      .map(r => {
        val arr: Array[String] = r.split(" ")
        (arr(0),arr(1).toLong*1000)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(10)) {
          override def extractTimestamp(element: (String, Long)): Long = (element._2)
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .allowedLateness(Time.seconds(10))
      .process(new UpdateWindowCountFunction)
    stream.print()
    env.execute()
  }

  class UpdateWindowCountFunction extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      //定义一个当前窗口可见的状态变量
      val isUpdate = context.windowState.getState(new ValueStateDescriptor[Boolean]("isUpdate",Types.of[Boolean]))
      if (!isUpdate.value()){
        out.collect("当到达水位线第一次触发窗口计算时"+elements.size+"个数据被计算")
        isUpdate.update(true)
      }else{
        out.collect("迟到数据来了，此时"+elements.size+"个数据被计算")
      }
    }
  }

}