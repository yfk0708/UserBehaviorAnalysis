package com.atguigu

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * 处理数据流中支付成功的订单和超时未支付的订单
 * 主流输出支付成功结果信息，侧输出流输出失效订单结果信息
 * 状态编程+定时器实现
 */

//订单数据样例类
case class OrderLog(orderId: Long, eventType: String, transactionId: String, timeStamp: Long)

//支付结果信息样例类
case class PayResult(orderId: Long, message: String)

object OrderTimeOut {
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

    val processedStream = dataStream
//     匹配只针对同一个订单
      .keyBy(_.orderId)
//    .process(new OrderPayMatch)
      .process(new OrderPayMatch1)
    processedStream.print("order pay match job")
    env.execute()
  }
}

/**
 * 订单创建与支付信息匹配
 * 第一种情况：先来create数据，判断pay数据是否来过，来过则匹配成功，没来过则注册定时器等15分钟，再不来则超时失败
 * 第二种情况：先来pay数据，判断create数据之前是否来过且是否在之前15分钟之内来的，是则匹配成功，否则超时失效
 */
class OrderPayMatch1 extends KeyedProcessFunction[Long, OrderLog, PayResult] {
  //  定义一个状态，保存订单数据的eventType情况(是不是pay)
  lazy val isPayedOrderState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))
  //  定义一个状态，保存定时器的触发时间
  lazy val timerTriggerTimeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timerTriggerTime", classOf[Long]))
  //  创建侧输出流标签，保存支付超时的订单数据
  private val outputTag = new OutputTag[PayResult]("timeOutOrder")

  override def processElement(value: OrderLog, ctx: KeyedProcessFunction[Long, OrderLog, PayResult]#Context, out: Collector[PayResult]): Unit = {
    val isPayedOrder = isPayedOrderState.value()
    val timerTriggerTime = timerTriggerTimeState.value()
    //    情况一：如果来的数据为create，再判断pay数据是否来了
    if (value.eventType == "create") {
      //    如果为true说明状态中已有pay数据，那么create和pay都来了，输出支付成功信息
      if (isPayedOrder) {
        out.collect(PayResult(value.orderId, "支付成功！"))
        //    删除定时器
        ctx.timerService().deleteEventTimeTimer(timerTriggerTime)
        isPayedOrderState.clear()
        timerTriggerTimeState.clear()
        //    否则为false时，说明没有pay数据，则注册定时器，在当前数据产生时间的15分钟后触发
      } else {
        val triggerTime = value.timeStamp * 1000 + 1000 * 60 * 15
        ctx.timerService().registerEventTimeTimer(triggerTime)
        timerTriggerTimeState.update(triggerTime)
      }
    }
    //    情况二：如果来的数据为pay，再判断create数据是否来过
    if (value.eventType == "pay") {
      //     如果定时器触发时间大于0，说明create数据来过(因为情况一中注册定时器的前提是create数据来了)，再判断来的时间
      if (timerTriggerTime > 0) {
        //    如果当前数据产生时间小于定时器触发时间，说明没有超出15分钟，输出支付成功信息
        if (value.timeStamp * 1000 < timerTriggerTime) {
          out.collect(PayResult(value.orderId, "支付成功！"))
          //   否则在侧输出流输出超时失效信息
        } else {
          ctx.output(outputTag, PayResult(value.orderId, "您在15分钟内未支付，订单已失效！"))
        }
        ctx.timerService().deleteEventTimeTimer(timerTriggerTime)
        isPayedOrderState.clear()
        timerTriggerTimeState.clear()
        //    不大于0，说明create数据没来过，则将当前数据的eventType更新到状态中，注册定时器，等create数据到来(数据乱序情况下)
      } else {
        isPayedOrderState.update(true)
        ctx.timerService().registerEventTimeTimer(value.timeStamp * 1000)
        timerTriggerTimeState.update(value.timeStamp * 1000)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, PayResult]#OnTimerContext, out: Collector[PayResult]): Unit = {
    val isPayedOrder = isPayedOrderState.value()
    //    如果为true，说明pay数据先到，create没等到
    if (isPayedOrder) {
      ctx.output(outputTag, PayResult(ctx.getCurrentKey, "订单已支付但订单创建日志丢失！"))
      //    create数据到了，没等到pay数据
    } else {
      ctx.output(outputTag, PayResult(ctx.getCurrentKey, "您在15分钟内未支付，订单已失效！"))
    }
    isPayedOrderState.clear()
    timerTriggerTimeState.clear()
  }
}

class OrderPayMatch extends KeyedProcessFunction[Long,OrderLog,PayResult]{
  //  定义一个状态，保存订单数据的eventType情况(是不是pay)
  lazy val isPayedOrderState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayed", classOf[Boolean]))

  override def processElement(value: OrderLog, ctx: KeyedProcessFunction[Long, OrderLog, PayResult]#Context, out: Collector[PayResult]): Unit = {
    val isPayedOrder = isPayedOrderState.value()
//    触发定时器后如果来的是create数据且状态中为false，没有来pay数据，则注册定时器等15分钟
    if (value.eventType=="create" && !isPayedOrder){
      ctx.timerService().registerEventTimeTimer(value.timeStamp*1000+1000*60*15)
    }
//    如果来的是pay数据，则将状态更新为true
    if (value.eventType=="pay"){
      isPayedOrderState.update(true)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderLog, PayResult]#OnTimerContext, out: Collector[PayResult]): Unit = {
    val isPayedOrder = isPayedOrderState.value()
//    触发定时器时，如果状态为true，有pay数据，则匹配成功，输出支付成功信息
    if (isPayedOrder){
      out.collect(PayResult(ctx.getCurrentKey,"支付成功！"))
//    否则匹配失败，输出超时信息
    } else {
      out.collect(PayResult(ctx.getCurrentKey,"您在15分钟内未支付，订单已失效！"))
    }
    isPayedOrderState.clear()
  }
}