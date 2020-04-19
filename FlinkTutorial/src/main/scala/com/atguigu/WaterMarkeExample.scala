package com.atguigu

import java.lang

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMarkeExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ds: DataStream[String] = env
      .socketTextStream("hadoop102", 9999, '\n')
      .map { r => {
        val a: Array[String] = r.split(" ")
        (a(0), a(1).toLong * 1000)
      }
      }
//      .assignTimestampsAndWatermarks(
//        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(10)) {
//          override def extractTimestamp(element: (String, Long)): Long = (element._2)
//        }
//      )
      .assignTimestampsAndWatermarks(new MyAssign)
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new PW)
    ds.print()
    env.execute()
  }
  class PW extends ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit =
      out.collect("窗口结束时间为 " + context.window.getEnd + " 的窗口闭合了！" + "元素数量是：" + elements.size + " 个")
  }

  class MyAssign extends AssignerWithPeriodicWatermarks[(String,Long)] {
    //自定义最大的延迟时间
    val time = 10*1000L
    //定义一个用于保存系统观察到的最大的事件时间
    var maxTs = Long.MinValue + time

    //系统插入水位线的时候调用
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs - time)
    }

    //每来一条数据都会触发一次
    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element._2)
      element._2
    }
  }
}