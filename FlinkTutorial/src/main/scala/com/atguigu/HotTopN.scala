package com.atguigu

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotTopN{
  case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
  case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .readTextFile("D:\\workspace_idea\\FlinkTutorial\\src\\main\\resources\\UserBehavior.txt")
      .map(r => {
        val str: Array[String] = r.split(",")
        UserBehavior(str(0).toLong,str(1).toLong,str(2).toInt,str(3),str(4).toLong*1000)
      })
      .filter(_.behavior == "pv")
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAgg,new WindowCountAgg)
      .keyBy(_.windowEnd)
      .process(new Hottop(3))
    stream.print()
    env.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class WindowCountAgg extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }
  }

  class Hottop(topsize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
    lazy val listStat = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("list",Types.of[ItemViewCount])
    )

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      listStat.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val data = new ListBuffer[ItemViewCount]()
      import scala.collection.JavaConversions._
      for (item <- listStat.get()){
        data += item
      }
      listStat.clear()
      val sortData: ListBuffer[ItemViewCount] = data.sortBy(-_.count).take(topsize)

      val result = new StringBuilder
      result
        .append("===============================\n")
        .append("时间:")
        .append(new Timestamp(timestamp))
        .append("\n")

      for (i <- sortData.indices){
        val item = sortData(i)
        result
          .append("No")
          .append(i+1)
          .append(":")
          .append("商品ID:")
          .append(item.itemId)
          .append("浏览量:")
          .append(item.count)
          .append("\n")
      }

      result.append("==================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }


}