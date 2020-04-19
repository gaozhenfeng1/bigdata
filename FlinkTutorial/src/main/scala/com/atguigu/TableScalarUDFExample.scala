package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction

object TableScalarUDFExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val stream = env.addSource(new SensorSource)

    val Tenv = StreamTableEnvironment.create(env,settings)

    val table = Tenv.fromDataStream(stream,'id)

    val hashCode =new HashCode(10)
    table
      .select('id,hashCode('id))
      .toAppendStream[(String,Int)]
//      .print()

    Tenv.registerFunction("hashCode",new HashCode(10))
    Tenv.createTemporaryView("t",table,'id)
    Tenv
        .sqlQuery("select id,hashCode(id) from t")
        .toAppendStream[(String,Int)]
        .print()


    env.execute()

  }

  class HashCode(factor: Int) extends ScalarFunction{
    def eval(s : String):Int ={
      s.hashCode*factor
    }
  }
}