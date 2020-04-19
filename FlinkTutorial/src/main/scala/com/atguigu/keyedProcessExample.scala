package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object keyedProcessExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .process(new TempIncreaseAlterFunction)
    ds.print()
    env.execute()
  }

  class TempIncreaseAlterFunction extends KeyedProcessFunction[String,SensorReading,String]{
    //状态变量，保存上一个传感器的温度值，仅当前key可见
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",Types.of[Double])
    )

    //保存注册的时间戳的状态变量
    lazy val currentTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",Types.of[Long])
    )

    //每来一个事件调用一次
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //取出状态变量上一次保存的温度值
      val preTemp: Double = lastTemp.value()
      //将当前的温度赋值给状态变量
      lastTemp.update(value.temperature)

      //取出保存的时间状态
      val timer: Long = currentTimer.value()

      //温度下降或者是第一个温度值，删除定时器
      if (preTemp ==0.0 || value.temperature<preTemp){
        ctx.timerService().deleteProcessingTimeTimer(timer)
        //清空状态变量
        currentTimer.clear()
      }else if (value.temperature>preTemp && timer == 0){
        //温度上升且没有报警事件存在
        //ts是下一秒时候的时间戳
        val ts = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(ts)
        currentTimer.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器id为"+ctx.getCurrentKey+"的传感器的温度已经连续一秒上升了")
      currentTimer.clear()
    }
  }
}