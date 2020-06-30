package com.atguigu

import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 处理数据流中支付成功的订单和超时未支付的订单
 * 主流输出支付成功的订单数据，侧输出流输出超时订单数据
 * 使用CEP API实现
 */

object OrderTimeOutWithCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(line => {
        val fields = line.split(",")
        val orderId = fields(0).toLong
        val eventType = fields(1)
        val transactionId = fields(2)
        val timeStamp = fields(3).toLong
        OrderLog(orderId, eventType, transactionId, timeStamp)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000)
      .keyBy(_.orderId)

//    创建匹配模式：15分钟内支付成功的数据
    val PayMatchPattern = Pattern.begin[OrderLog]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))
//    将数据流应用该匹配模式
    val patternStream = CEP.pattern(dataStream, PayMatchPattern)
//    创建侧输出流标签，保存超时的数据流
    val outputTag = new OutputTag[PayResult]("timeOutOrder")
//    选取匹配到的数据流，包括超时未支付的数据和支付成功的数据(注意：超时数据在前)
    val resultStream = patternStream.select(outputTag,new TimeOutMatch,new PaySuccessMatch)
    resultStream.print("payed order")
    resultStream.getSideOutput(outputTag).print("time out order")
    env.execute()
  }
}

//模式选取超时未支付的数据
class TimeOutMatch extends PatternTimeoutFunction[OrderLog,PayResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderLog]], timeoutTimestamp: Long): PayResult = {
//    超时的数据需要选取begin的orderId，因为没有匹配到follow的数据
    val orderId = pattern.get("begin").iterator().next().orderId
    PayResult(orderId,"您在15分钟内未支付，订单已失效！")
  }
}

//模式选取支付成功的数据
class PaySuccessMatch extends PatternSelectFunction[OrderLog,PayResult]{
  override def select(pattern: util.Map[String, util.List[OrderLog]]): PayResult = {
    val orderId = pattern.get("follow").iterator().next().orderId
    PayResult(orderId,"支付成功！")
  }
}