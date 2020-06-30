package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 对2秒内连续两次登录失败的数据输出报警信息
 * 状态编程+定时器实现
 */

//输入数据样例类(用户登录日志)
case class LoginLog(userId: Long, ip: String, eventType: String, timeStamp: Long)

//输出数据样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, message: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resuorce = getClass.getResource("/LoginLog.csv")
    val dataStream = env.readTextFile(resuorce.getPath)
      .map(line => {
        val fields = line.split(",")
        val userId = fields(0).toLong
        val ip = fields(1)
        val eventType = fields(2)
        val timeStamp = fields(3).toLong
        LoginLog(userId, ip, eventType, timeStamp)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginLog](Time.seconds(5)) {
        override def extractTimestamp(element: LoginLog): Long = element.timeStamp * 1000
      })

    val processedStream = dataStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))
    processedStream.print()
    env.execute("Continuous login failure job")
  }
}

class LoginFailWarning(maxFailCount: Int) extends KeyedProcessFunction[Long, LoginLog, Warning] {
  //  定义一个状态，保存在2秒内登录失败的数据列表
  lazy val loginFailLogsState: ListState[LoginLog] = getRuntimeContext.getListState(new ListStateDescriptor[LoginLog]("loginFailLogs", classOf[LoginLog]))

  override def processElement(value: LoginLog, ctx: KeyedProcessFunction[Long, LoginLog, Warning]#Context, out: Collector[Warning]): Unit = {
    val loginFailLogs = loginFailLogsState.get()
    //    如果数据的eventType为fail，则添加到状态中，否则清空状态
    if (value.eventType == "fail") {
      //      如果状态中没有数据,说明是第一次处理，则注册定时器，触发时间为当前数据产生时间的2秒后
      if (!loginFailLogs.iterator().hasNext) {
        ctx.timerService().registerEventTimeTimer(value.timeStamp * 1000 + 2000)
      }
      loginFailLogsState.add(value)
    } else {
      loginFailLogsState.clear()
    }
  }

  //  触发定时器时，根据状态里的失败个数决定是否输出报警信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginLog, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //    创建一个列表，用来保存登录失败的数据
    val loginFailLogsList = new ListBuffer[LoginLog]
    //    从状态中取出数据
    val loginFailLogs = loginFailLogsState.get().iterator()
    //    遍历添加到列表中
    while (loginFailLogs.hasNext) {
      loginFailLogsList += loginFailLogs.next()
    }
    //    如果列表长度大于最大失败次数限度，则包装成Warning输出
    if (loginFailLogsList.length >= maxFailCount) {
      val userId = loginFailLogsList.head.userId
      val firstFailTime = loginFailLogsList.head.timeStamp
      val lastFailTime = loginFailLogsList.last.timeStamp
      val message = "2秒内登录失败次数为" + loginFailLogsList.length
      out.collect(Warning(userId, firstFailTime, lastFailTime, message))
    }
    loginFailLogsState.clear()
  }
}