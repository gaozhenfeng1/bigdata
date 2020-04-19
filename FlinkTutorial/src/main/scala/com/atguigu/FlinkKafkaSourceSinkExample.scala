package com.atguigu

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object FlinkKafkaSourceSinkExample {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env
      .addSource(new FlinkKafkaConsumer011[String]("flink",new SimpleStringSchema(),properties))
    stream.print()

    stream.addSink(new FlinkKafkaProducer011[String]("192.168.1.102:9092","flink",new SimpleStringSchema()))

    env.execute()

  }
}