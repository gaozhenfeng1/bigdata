package com.atguigu.test

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafkaProducerEx {

  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }

  def writeToKafka(topic: String): Unit = {
    val pro = new Properties()
    pro.put("bootstrap.servers","hadoop102:9092")
    pro.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    pro.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val kafkaproducer = new KafkaProducer[String,String](pro)

    val file = io.Source.fromFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\UserBehavior.txt")

    for (line <- file.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      kafkaproducer.send(record)
    }

    kafkaproducer.close()
  }


}
