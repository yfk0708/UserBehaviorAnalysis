package com.atguigu

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


/**
 * 处理已支付订单日志数据和到账日志数据
 * 判断在订单数据到后的5秒内对应的到账数据是否也到了(或相反)
 * 到了则输出匹配成功，没到则输出报警信息
 */

//到账所产生的信息样例类
case class ReceiptLog(transactionId: String, payChannel: String, timeStamp: Long)

object TransactionMatchDetect {
  //  创建侧输出流标签，保存未匹配到的数据
  val unMatchedPays = new OutputTag[OrderLog]("unMatchedPays")
  val unMatchedReceipts = new OutputTag[ReceiptLog]("unMatchedReceipts")

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

    //    读取到账日志数据流
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

    //    连接两条数据流，共同处理
    val processedStream = payedStream.connect(receiptStream).process(new TransactionDetect)
    processedStream.print("matchedOrder")
    processedStream.getSideOutput(unMatchedPays).print("unMatchedPays")
    processedStream.getSideOutput(unMatchedReceipts).print("unMatchedReceipts")
    env.execute()
  }


  class TransactionDetect extends CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)] {
    //  定义两个状态，保存已经到达的两条流的数据
    lazy val payedOrderLogState = getRuntimeContext.getState(new ValueStateDescriptor[OrderLog]("payedOrderLog", classOf[OrderLog]))
    lazy val receiptLogState = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptLog]("receiptLog", classOf[ReceiptLog]))

    //  处理订单日志数据
    override def processElement1(value: OrderLog, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#Context, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
      //   获取到账日志数据
      val receiptLog = receiptLogState.value()
      //    如果有对应的到账日志数据，则在主流输出匹配信息
      if (receiptLog != null) {
        out.collect((value, receiptLog))
        receiptLogState.clear()
      } else {
        //   否则把已支付订单日志存入状态，注册定时器，等待对应的到账日志数据
        payedOrderLogState.update(value)
        ctx.timerService().registerEventTimeTimer(value.timeStamp * 1000 + 5000)
      }
    }

    //  处理到账日志数据
    override def processElement2(value: ReceiptLog, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#Context, out: Collector[(OrderLog, ReceiptLog)]): Unit = {
      val orderLog = payedOrderLogState.value()
      //    如果有对应的已支付订单日志数据，则在主流输出匹配信息
      if (orderLog != null) {
        out.collect((orderLog, value))
        payedOrderLogState.clear()
      } else {
        //    否则把到账日志数据存入状态，注册定时器，等待对应的订单日志数据
        receiptLogState.update(value)
        ctx.timerService().registerEventTimeTimer(value.timeStamp * 1000 + 5000)
      }
    }

    //   如果触发定时器后，还没有收到对应的数据，则输出报警信息
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderLog, ReceiptLog, (OrderLog, ReceiptLog)]#OnTimerContext, out: Collector[(OrderLog, ReceiptLog)]): Unit = {

      //    有已支付订单日志，没有到账日志
      if (payedOrderLogState.value() != null) {
        ctx.output(unMatchedPays, payedOrderLogState.value())
      }
      //    有到账日志，没有已支付订单日志
      if (receiptLogState.value() != null) {
        ctx.output(unMatchedReceipts, receiptLogState.value())
      }
      payedOrderLogState.clear()
      receiptLogState.clear()
    }
  }
}


