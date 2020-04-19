package com.atguigu

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object TableAggregateFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env, settings)
    val myAggFunc = new MyMinMax
    val stream = env.fromElements(
      (1, -2),
      (1, 2)
    )
    val table = tEnv.fromDataStream(stream, 'key, 'a)
  }

  case class MyMinMaxAcc(var min:Int,var max:Int)
  class MyMinMax extends AggregateFunction[Row,MyMinMaxAcc] {
    override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(Int.MaxValue,Int.MinValue)



    override def getValue(accumulator: MyMinMaxAcc): Row = ???
  }
}
