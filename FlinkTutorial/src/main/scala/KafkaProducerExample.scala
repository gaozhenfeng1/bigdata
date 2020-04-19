package com.atguigu

import java.util.Properties

import org.apache.kafka.clients.producer._

object KafkaProducerExample {
  def main(args: Array[String]): Unit = {
    WriteToKafka("test")
  }

  def WriteToKafka(topic:String):Unit ={
    val pro = new Properties()
    pro.put("bootstrap.servers","192.168.1.102:9092")
    pro.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    pro.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](pro)

    val bufferSource = io.Source.fromFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\scala\\1.txt")

    for (line <- bufferSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }

    producer.close()
  }

}