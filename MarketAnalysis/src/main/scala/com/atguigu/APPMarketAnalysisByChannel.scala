package com.atguigu

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 统计APP最近1小时在各种渠道产生的各种操作行为数据条数，每10秒钟更新一次
 * 用process实现
 */

//  APP市场用户行为数据样例类
case class APPMarketUserBehaviorLog(userId: String, behavior: String, channel: String, timeStamp: Long)

//  统计结果样例类
case class StatisticResult(windowStart: String, windowEnd: String, behavior: String, channel: String, count: Long)

object APPMarketAnalysisByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timeStamp)

    val processedStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      //      转换成以behavior和channel为key的tuple
      .map(line => ((line.behavior, line.channel), 1L))
      //      根据key分组
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new GetResultByWindow)
    processedStream.print()
    env.execute()
  }
}

//模拟生成数据源
class SimulatedEventSource extends SourceFunction[APPMarketUserBehaviorLog] {
  //  定义运行状态，表示数据源是否正常运行
  private var isRunning = true
  //  用户行为类型
  private val behaviorsType = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  //  渠道类型
  private val channelsType = Seq("web", "APPStore", "weChat", "weiBo", "taoBao")
  private val random = Random

  override def run(ctx: SourceFunction.SourceContext[APPMarketUserBehaviorLog]): Unit = {
    //  设定最多造多少条数据
    val maxElements = Long.MaxValue
    //  用来统计已造数据的条数
    var count = 0L
    //  如果正常运行且没达到数据最大条数上限，就一直造数据
    while (isRunning && count < maxElements) {
      //    随机生成字段，包装成样例类输出
      val userId = UUID.randomUUID().toString
      //    随机选取Seq中的一个元素作
      val behavior = behaviorsType(random.nextInt(behaviorsType.size))
      val channel = channelsType(random.nextInt(channelsType.size))
      val timeStamp = System.currentTimeMillis()
      ctx.collect(APPMarketUserBehaviorLog(userId, behavior, channel, timeStamp))
      //    生成一条累加一次
      count += 1
      //    设置生成数据时间间隔
      TimeUnit.NANOSECONDS.sleep(5)
    }
  }

  //运行状态设置为false，则取消生成数据
  override def cancel(): Unit = isRunning = false
}

//将每条数据包装成MarketCountResult输出
class GetResultByWindow extends ProcessWindowFunction[((String, String), Long), StatisticResult, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[StatisticResult]): Unit = {
    val windowStart = new Timestamp(context.window.getStart).toString
    val windowEnd = new Timestamp(context.window.getEnd).toString
    val behavior = key._1
    val channel = key._2
    val count = elements.size
    out.collect(StatisticResult(windowStart, windowEnd, behavior, channel, count))
  }
}