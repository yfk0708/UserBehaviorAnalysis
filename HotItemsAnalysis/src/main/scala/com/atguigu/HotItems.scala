package com.atguigu

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 按浏览量统计最近1小时排名topN的热门商品，每5分钟更新一次
 */
//埋点用户行为数据样例类
case class UserBehaviorLog(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)
//窗口聚合结果样例类
case class ItemPVLog(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//      读取数据(相对路径)
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
//     由于数据的时间戳是有序的，所以只分配timeStamp不需要watermark
      .assignAscendingTimestamps(_.timeStamp * 1000)

//    过滤、分组、开窗、聚合、分组、取topN
    val processedStream = dataStream
      .filter(_.behavior == "pv")
//      注意：须使用“_.”写法，否则窗口函数中的key为Tuple类型
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new GetAggResult, new GetItemPVLogByWindow)
      .keyBy(_.windowEnd)
      .process(new GetTopNItemsByWindow(5))
    processedStream.print()
    env.execute()
  }
}


//自定义预聚合函数，求每个窗口的数据条数
class GetAggResult extends AggregateFunction[UserBehaviorLog, Long, Long] {
  //  设置累加器初始值：0条数据
  override def createAccumulator(): Long = 0L
  //  累加方式：来一条数据累加一次
  override def add(value: UserBehaviorLog, accumulator: Long): Long = accumulator + 1
  //  获得累加结果：数据总条数
  override def getResult(accumulator: Long): Long = accumulator
  //  合并累加器方式：合并累加结果
  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口函数，按窗口输出结果信息
class GetItemPVLogByWindow extends WindowFunction[Long, ItemPVLog, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemPVLog]): Unit = {
//   key用分组时的key，窗口时间用此条数据所在窗口的windowEnd，count用上一步的聚合结果
    out.collect(ItemPVLog(key, window.getEnd, input.iterator.next()))
  }
}

//自定义处理函数，按窗口获取输出结果信息的前n条
class GetTopNItemsByWindow(topSize: Int) extends KeyedProcessFunction[Long, ItemPVLog, String] {
  //  定义一个状态列表，保存每个窗口所有的ItemPVLog
  lazy val ItemPVLogsState: ListState[ItemPVLog] = getRuntimeContext.getListState(new ListStateDescriptor[ItemPVLog]("itemPVLogs", classOf[ItemPVLog]))

  override def processElement(value: ItemPVLog, ctx: KeyedProcessFunction[Long, ItemPVLog, String]#Context, out: Collector[String]): Unit = {
    //    每来一条数据就存入状态列表
    ItemPVLogsState.add(value)
    //    注册一个定时器，在当前数据所在窗口的1秒后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //  触发定时器后的操作(对所有数据排序，并输出结果)
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemPVLog, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    创建一个列表保存状态中的数据
    val itemPVLogsList:ListBuffer[ItemPVLog] = new ListBuffer
    //    遍历集合需导入此包
    import scala.collection.JavaConversions._
    //    遍历ItemPVLogsState，将每条数据添加到列表
    for (itemPVLog <- ItemPVLogsState.get()) {
      itemPVLogsList+=itemPVLog
    }
    //    按照每条数据的count降序排序，取前n个
    val topNItemPVLogs = itemPVLogsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    ItemPVLogsState.clear()
    //    将排名结果格式化输出
    val result:StringBuilder = new StringBuilder
    result.append("时间:").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- topNItemPVLogs.indices) {
      val currentOutPutLog = topNItemPVLogs(i)
      result.append("No.").append(i + 1).append(":")
        .append(" 商品id=").append(currentOutPutLog.itemId)
        .append(" 浏览量=").append(currentOutPutLog.count)
        .append("\n")
    }
    result.append("-----------------------------")
    //    设置每条数据的输出间隔时间
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}