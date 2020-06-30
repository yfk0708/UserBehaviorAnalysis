package com.atguigu

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 统计最近10分钟浏览量排名topN的页面，5秒钟刷新一次
 */

//  服务器日志样例类
case class ApacheEventLog(ip: String, userId: String, eventTime: Long, method: String, url: String)

//  窗口聚合结果样例类(输出显示在窗口中的类型)
case class UrlPVResult(url: String, windowEnd: Long, count: Long)

object HotWebPages {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/apache.log")
    val dataStream = env.readTextFile(resource.getPath)
      .map(line => {
        val fields = line.split(" ")
        val ip = fields(0)
        val userId = fields(1)
        //        将时间字段转换成指定格式
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val eventTime = simpleDateFormat.parse(fields(3)).getTime
        val method = fields(5)
        val url = fields(6)
        ApacheEventLog(ip, userId, eventTime, method, url)
      })
      //      数据源时间有乱序，所以使用watermark，最大乱序值大约为1分钟，设置1分钟延迟
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheEventLog](Time.minutes(1)) {
        //      将数据中的eventTime作为timestamp，单位若为秒则须乘以1000
        override def extractTimestamp(element: ApacheEventLog): Long = element.eventTime
      })
    val processedStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //      允许1分钟的迟到数据
      .allowedLateness(Time.minutes(1))
      .aggregate(new GetAggResult1, new GetUrlPVResultByWindow)
      .keyBy(_.windowEnd)
      .process(new GetTopNHotWebPages(5))
    processedStream.print()
    env.execute("hot webPages job")
  }
}

//自定义预聚合函数，求每个窗口的数据条数
class GetAggResult1 extends AggregateFunction[ApacheEventLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheEventLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，按窗口输出结果信息
class GetUrlPVResultByWindow extends WindowFunction[Long, UrlPVResult, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlPVResult]): Unit = {
    out.collect(UrlPVResult(key, window.getEnd, input.iterator.next()))
  }
}

class GetTopNHotWebPages(topSize: Int) extends KeyedProcessFunction[Long, UrlPVResult, String] {
  //  定义一个状态，保存UrlPVResult
  lazy val urlPVResultsState = getRuntimeContext.getListState(new ListStateDescriptor[UrlPVResult]("urlPVResults", classOf[UrlPVResult]))

  override def processElement(value: UrlPVResult, ctx: KeyedProcessFunction[Long, UrlPVResult, String]#Context, out: Collector[String]): Unit = {
    //  把每条数据存入状态列表
    urlPVResultsState.add(value)
    //  注册定时器，触发时间为当前数据所在窗口时间1秒后
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlPVResult, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    将所有state中的数据取出，放到一个ListBuffer中
    val allUrlPVResult = new ListBuffer[UrlPVResult]
    //    用迭代器遍历
    val stateIterator = urlPVResultsState.get().iterator()
    while (stateIterator.hasNext) {
      allUrlPVResult += stateIterator.next()
    }
    //    按照count降序排序，并取前n个
    val sortedUrlPVResult = allUrlPVResult.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    urlPVResultsState.clear()
    //    将排名结果格式化输出
    val result = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    //    输出每个页面的信息
    for (i <- sortedUrlPVResult.indices) {
      val currentUrl = sortedUrlPVResult(i)
      result.append("NO.").append(i + 1).append(":")
        .append(" URL=").append(currentUrl.url)
        .append(" 访问量=").append(currentUrl.count)
        .append("\n")
    }
    result.append("----------------------------")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}