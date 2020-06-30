package com.atguigu

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 求最近1小时的访问用户量（UV）
 * apply和aggregate窗口函数两种处理方式
 */

//UV结果信息样例类
case class UVCountResult(windowEnd: Long, count: Long)

object UniqueVisitor {
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
    //    过滤、开窗、聚合
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
    //      .apply(new GetUVCountResultByWindow)
      .aggregate(new GetAggResult, new GetUVCountResultByWindow1)
    processedStream.print()
    env.execute("uv job")
  }
}

//自定义窗口函数，按窗口输出UV结果信息
class GetUVCountResultByWindow extends AllWindowFunction[UserBehaviorLog, UVCountResult, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehaviorLog], out: Collector[UVCountResult]): Unit = {
    //  创建Set集合，只保存数据的userId，用来去重
    var userIdSet = Set[Long]()
    //  遍历每条数据，将userId添加到Set中
    for (userBehaviorLog <- input) {
      userIdSet += userBehaviorLog.userId
    }
    //  输出每条数据，userId已去重
    out.collect(UVCountResult(window.getEnd, userIdSet.size))
  }
}

//自定义聚合函数，求每个窗口的数据条数(已根据数据的userId去重)
class GetAggResult extends AggregateFunction[UserBehaviorLog, Set[Long], Long] {
//  设置累加器初始值：一个Set
  override def createAccumulator(): Set[Long] = Set[Long]()
//  累加方式：累加每条数据的userId
  override def add(value: UserBehaviorLog, accumulator: Set[Long]): Set[Long] = accumulator+value.userId
//  获得累加结果：获取Set的大小
  override def getResult(accumulator: Set[Long]): Long = accumulator.size
//  合并累加器方式：合并两个Set
  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a++b
}

//自定义窗口函数，封装成UVCountResult输出
class GetUVCountResultByWindow1 extends AllWindowFunction[Long,UVCountResult,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UVCountResult]): Unit = {
    out.collect(UVCountResult(window.getEnd,input.size))
  }
}