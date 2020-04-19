package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TriggerExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new MyTrigger)
      .process(new MyProcessWindow)
    stream.print()
    env.execute()
  }

  class MyProcessWindow extends ProcessWindowFunction[SensorReading,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("时间戳为："+ context.currentProcessingTime + "触发了窗口计算，" + "窗口一共：" + elements.size + " 条数据")
    }
  }

  class MyTrigger extends Trigger[SensorReading,TimeWindow] {

    override def onElement(element: SensorReading, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",Types.of[Boolean])
      )
      if (!firstSeen.value()){
        val ts = ctx.getCurrentProcessingTime+(1000-(ctx.getCurrentProcessingTime%1000))
        ctx.registerProcessingTimeTimer(ts)
        ctx.registerProcessingTimeTimer(window.getEnd)
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      if (time == window.getEnd){
        TriggerResult.FIRE_AND_PURGE
      }else{
        val t = ctx.getCurrentProcessingTime+(1000-(ctx.getCurrentProcessingTime%1000))
        if (t<window.getEnd){
          ctx.registerProcessingTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }

}