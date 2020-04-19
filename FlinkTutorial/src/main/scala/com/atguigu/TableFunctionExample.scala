package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val Tenv = StreamTableEnvironment.create(env,settings)

    val stream = env.fromElements(
      "hello#world",
      "atguigu#gaozhenfeng"
    )

    val table = Tenv.fromDataStream(stream,'s)

    val split = new Split("#")
    table
        .joinLateral(split('s) as ('word,'length))
        .select('s,'word,'length)
        .toAppendStream[(String,String,Long)]
//        .print()

    Tenv.registerFunction("split",new Split("#"))
    Tenv.createTemporaryView("t",stream,'s)
    Tenv
        .sqlQuery("SELECT s,word,length FROM t, LATERAL TABLE(split(s)) as T(word,length)")
        .toAppendStream[(String,String,Long)]
        .print()

    env.execute()
  }

  class Split(separator:String) extends TableFunction[(String,Int)]{
    def eval(str:String)={
      str.split(separator).foreach(x => collect((x,x.length)))
    }
  }

}