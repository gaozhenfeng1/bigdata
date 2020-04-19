package com.atguigu

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MixAggregateAndProcess {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[MinMaxTemp] = env
      .addSource(new SensorSource)
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new Agg, new ProcessWindow)
    ds.print()
    env.execute()
  }

  case class MinMaxTemp (id:String , Mintemp:Double , MaxTemp:Double , endTs:Long)

  class Agg extends AggregateFunction[SensorReading,(Double,Double),(Double,Double)] {
    override def createAccumulator(): (Double, Double) = (Double.MaxValue,Double.MinValue)

    override def add(value: SensorReading, accumulator: (Double, Double)): (Double, Double) =
      ((value.temperature.min(accumulator._1),value.temperature.max(accumulator._2)))

    override def getResult(accumulator: (Double, Double)): (Double, Double) = accumulator

    override def merge(a: (Double, Double), b: (Double, Double)): (Double, Double) =
      ((a._1.min(b._1),a._2.max(b._2)))
  }

  class ProcessWindow extends ProcessWindowFunction[(Double,Double),MinMaxTemp,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val minMax: (Double, Double) = elements.head
      out.collect(MinMaxTemp(key,minMax._1,minMax._2,context.window.getEnd))
    }
  }
}