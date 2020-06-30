package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 求最近1小时的页面浏览量（PV）
 */

//埋点用户行为数据样例类
case class UserBehaviorLog(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(line => {
        val fields = line.split(",")
        val userId = fields(0).toLong
        val itemId = fields(1).toLong
        val categoryId = fields(2).toInt
        val behavior = fields(3)
        val timeStamp = fields(4).toLong
        UserBehaviorLog(userId, itemId, categoryId, behavior, timeStamp)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000)

    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .map(line => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
//    只输出PV数
//      将每条数据转换成1，代表1次浏览
//      .map(line =>1)
//      .timeWindowAll(Time.hours(1))
//      求每个窗口的页面浏览量
//      .sum(0)
    processedStream.print()
    env.execute("pv job")
  }
}