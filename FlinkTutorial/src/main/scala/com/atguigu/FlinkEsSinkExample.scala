package com.atguigu

import java.util

import com.atguigu.utils.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object FlinkEsSinkExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("192.168.1.102",9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHost,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val json = new util.HashMap[String,String]()
          json.put("data",t.toString)
          val indexRequest = Requests
            .indexRequest()
            .index("sensor")
            .`type`("readingData")
            .source(json)
          requestIndexer.add(indexRequest)
        }
      }

    )

    val stream = env.addSource(new SensorSource)
    esSinkBuilder.setBulkFlushMaxActions(1)
    stream.addSink(esSinkBuilder.build())
    env.execute()

  }
}