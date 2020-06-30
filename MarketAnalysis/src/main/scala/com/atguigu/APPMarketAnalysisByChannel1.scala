package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 统计APP最近1小时在各种渠道产生的各种操作行为数据条数，每10分钟更新一次
 * 用aggregate实现
 */

object APPMarketAnalysisByChannel1 {
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
      .aggregate(new GetAggResult,new GetResultByWindow1)
    processedStream.print()
    env.execute()
  }
}

//自定义预聚合函数，求每个窗口的数据条数
class GetAggResult extends AggregateFunction[((String,String),Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ((String,String),Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，按窗口输出结果信息
class GetResultByWindow1 extends WindowFunction[Long,StatisticResult,(String,String),TimeWindow]{
  override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[StatisticResult]): Unit = {
    val windowStart = window.getStart.toString
    val windowEnd = window.getEnd.toString
    val behavior = key._1
    val channel = key._2
    val count = input.iterator.next()
    out.collect(StatisticResult(windowStart,windowEnd,behavior,channel,count))
  }
}