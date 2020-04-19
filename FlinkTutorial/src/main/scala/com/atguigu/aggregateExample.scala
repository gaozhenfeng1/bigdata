package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object aggregateExample{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[(String, Double)] = env
      .addSource(new SensorSource)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempFunction)
    ds.print()
    env.execute()
  }

   class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{
    //定义一个空的累加器
    override def createAccumulator(): (String, Double, Int) = ("",0.0,0)
    //数据来了之后与累加器的数据结合
    override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) =
      (value._1,value._2+accumulator._2,accumulator._3+1)
    //获得最终的结果
    override def getResult(accumulator: (String, Double, Int)): (String, Double) =
      (accumulator._1,accumulator._2/accumulator._3)
    //两个累加器之间的操作
    override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) =
      (a._1,a._2+b._2,a._3+b._3)
  }

}