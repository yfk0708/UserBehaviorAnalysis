package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 处理已支付订单日志数据和到账日志数据
 * 检测在订单数据到后的5秒内对应的到账数据是否也到了(或相反)
 * 到了则输出匹配成功，没到则输出报警信息
 * 使用intervalJoin函数处理两条数据流
 */

object TransactionMatchByJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    读取已支付的订单日志数据流
    val resource = getClass.getResource("/OrderLog.csv")
    val payedStream = env.readTextFile(resource.getPath)
      .map(line => {
        val fields = line.split(",")
        val orderId = fields(0).toLong
        val eventType = fields(1)
        val transactionId = fields(2)
        val timeStamp = fields(3).toLong
        OrderLog(orderId, eventType, transactionId, timeStamp)
      })
      //    保留所有支付成功的数据
      .filter(_.transactionId != "")
      .assignAscendingTimestamps(_.timeStamp * 1000)
      .keyBy(_.transactionId)

    //   读取到账日志数据流
    val resource1 = getClass.getResource("/ReceiptLog.csv")
    val receiptStream = env.readTextFile(resource1.getPath)
      .map(line => {
        val fields = line.split(",")
        val transactionId = fields(0)
        val payChannel = fields(1)
        val timeStamp = fields(2).toLong
        ReceiptLog(transactionId, payChannel, timeStamp)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000)
      .keyBy(_.transactionId)

    //   通过join连接两条数据流
    payedStream.intervalJoin(receiptStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TransactionDetect1)
  }
}

class TransactionDetect1 extends ProcessJoinFunction[OrderLog,ReceiptLog,(OrderLog,ReceiptLog)]{
  override def processElement(left: OrderLog, right: ReceiptLog, ctx: ProcessJoinFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#Context, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
    out.collect((left,right))
  }
}
