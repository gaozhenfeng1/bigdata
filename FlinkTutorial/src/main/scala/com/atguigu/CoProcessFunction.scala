package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val filterSwitch = env.fromCollection(Seq(
      ("Sensor2",5*1000L),
      ("Sensor7",10*1000L)
    )).keyBy(_._1)

    val stream = env.addSource(new SensorSource).keyBy(_.id)

    val reading = stream.connect(filterSwitch).process(new ReadingFilter)

    reading.print()

    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading,(String,Long),SensorReading] {
    //定义了一个状态开关
    lazy val forwordEnable =
      getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean]))
    //第一条流的处理，第一条流的每一个元素只有当开关打开的时候才会输出
    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (forwordEnable.value()){
        out.collect(value)
      }
    }
    //处理第二条流的元素，当第二条流来的时候打开开关并设置一个定时任务多少秒后关闭开关
    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      forwordEnable.update(true)
      val ts = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    //定时任务的处理
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      forwordEnable.update(false)
    }

  }

}