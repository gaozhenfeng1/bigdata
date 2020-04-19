package com.atguigu

import com.atguigu.utils.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._


object tableApiExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    val table: Table = tenv.fromDataStream(stream)

    table
      .select('id,'temperature)
      .filter("id = 'sensor_1'")
      .toAppendStream[(String,Double)]
      .print()

    env.execute()
  }
}