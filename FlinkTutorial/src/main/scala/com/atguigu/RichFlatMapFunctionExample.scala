package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object RichFlatMapFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .flatMap(new MyFlatmap(1.7))
    stream.print()
    env.execute()
  }

  class MyFlatmap(val diff: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
    //定义一个温度状态
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",Types.of[Double])
    )
    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
      val ltemp = lastTemp.value()
      val diftemp = (value.temperature-ltemp).abs
      if (diftemp > diff){
        out.collect((value.id,value.temperature,diftemp))
      }
      lastTemp.update(value.temperature)
    }
  }

}