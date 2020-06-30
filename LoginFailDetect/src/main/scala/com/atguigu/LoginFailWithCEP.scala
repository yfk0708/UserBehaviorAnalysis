package com.atguigu

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCEP {
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
      .keyBy(_.userId)

    //    创建匹配模式：2秒内连续两次登录失败的数据
    val loginFailPattern = Pattern.begin[LoginLog]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))
    //    将数据流应用该匹配模式
    val patternedStream = CEP.pattern(dataStream, loginFailPattern)
    //    选取匹配到的数据流
    val processedStream = patternedStream.select(new LoginFailMatch)
    processedStream.print()
    env.execute()
  }
}

class LoginFailMatch extends PatternSelectFunction[LoginLog,Warning] {
//  模式所在阶段名称为key,匹配到的数据为value
  override def select(patternMap: util.Map[String, util.List[LoginLog]]): Warning = {
//    从模式中提取所需字段，包装成Warning输出
    val firstFailLog = patternMap.get("begin").iterator().next()
    val lastFailLog = patternMap.get("next").iterator().next()
    val userId = firstFailLog.userId
    val firstFailTime = firstFailLog.timeStamp
    val lastFailTime = lastFailLog.timeStamp
    val message = "2秒内已连续登录失败2次"
    Warning(userId,firstFailTime,lastFailTime,message)
  }
}