package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 统计最近1小时广告在各省份的点击量，每10秒钟更新一次
 * 如果某用户点击单条广告超过100次，则输出报警信息，并将用户加入黑名单1天
 */
//广告点击日志样例类
case class AdvertisingClickLog(userId: Long, advertisingId: Long, province: String, city: String, timeStamp: Long)

//统计结果样例类
case class StatisticResult1(windowEnd: String, province: String, count: Long)

//恶意点击报警信息样例类
case class ClickExceedWarning(userId: Long, advertisingId: Long, message: String)

object AdClickStatisticByProvince {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/AdClickLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(line => {
        val fields = line.split(",")
        val userId = fields(0).toLong
        val advertisingId = fields(1).toLong
        val province = fields(2)
        val city = fields(3)
        val timeStamp = fields(4).toLong
        AdvertisingClickLog(userId, advertisingId, province, city, timeStamp)
      })
      .assignAscendingTimestamps(_.timeStamp*1000)
//     根据用户id和广告id分组，过滤点击广告超过100次的用户
    val filteredStream = dataStream
      .keyBy(line => (line.userId, line.advertisingId))
      .process(new FilterClicksExceedUser(100))

    val processedStream = filteredStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new GetAggResult1, new GetResultByWindow2)
      processedStream.print()
      env.execute()
  }
}

class GetAggResult1 extends AggregateFunction[AdvertisingClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdvertisingClickLog, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class GetResultByWindow2 extends WindowFunction[Long, StatisticResult1, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[StatisticResult1]): Unit = {
    out.collect(StatisticResult1(window.getEnd.toString,key,input.iterator.next()))
  }
}

//过滤点击次数超标的用户
class FilterClicksExceedUser(maxClicks: Int) extends KeyedProcessFunction[(Long, Long), AdvertisingClickLog, AdvertisingClickLog] {
  //    定义一个状态，保存当前用户对当前广告的点击量
  lazy val currentClicksState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("clickCount", classOf[Long]))
  //    定义一个状态，表示是否发送过黑名单
  lazy val isSentBlackListState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSentBlackList", classOf[Boolean]))
  //    定义一个状态，保存定时器的触发时间
  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimer", classOf[Long]))

  lazy val blackListTag = new OutputTag[ClickExceedWarning]("blackList")
  override def processElement(value: AdvertisingClickLog, ctx: KeyedProcessFunction[(Long, Long), AdvertisingClickLog, AdvertisingClickLog]#Context, out: Collector[AdvertisingClickLog]): Unit = {
    //    获得当前点击量
    val currentClicks = currentClicksState.value()
    //    如果当前点击量为0，说明是第一次处理，则设置触发时间为每天00:00
    if (currentClicks == 0) {
      val triggerTime = ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24 + 1) * (1000 * 60 * 60 * 24)
      resetTimerState.update(triggerTime)
      //  注册定时器，到时间则触发
      ctx.timerService().registerProcessingTimeTimer(triggerTime)
    }
    //    如果当前点击量达到最大次数上限，则加入黑名单，否则点击量照常累加
    if (currentClicks >= maxClicks) {
      if (!isSentBlackListState.value()) {
        isSentBlackListState.update(true)
        ctx.output(blackListTag, ClickExceedWarning(value.userId, value.advertisingId, "点击次数已超过最大上限，请于1天后重试"))
      }
      return
    }
    //    点击量加1，输出到主流
    currentClicksState.update(currentClicks + 1)
    out.collect(value)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdvertisingClickLog, AdvertisingClickLog]#OnTimerContext, out: Collector[AdvertisingClickLog]): Unit = {
    //    定时器触发时，清空状态
    if (timestamp==resetTimerState.value()){
      isSentBlackListState.clear()
      currentClicksState.clear()
      resetTimerState.clear()
    }
  }
}