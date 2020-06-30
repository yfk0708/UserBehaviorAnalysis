package com.atguigu

import java.sql.Timestamp

import com.atguigu.HotItems.getClass
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * 统计最近1小时排名topN的热门商品，每5分钟更新一次
  * Flink SQL实现
  */
object HotItemsWithSQL {
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
    //    创建flink table执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //    将数据流转换成临时表(字段名不能与SQL关键字重叠)
    tableEnv.createTemporaryView("agg",dataStream,'itemId, 'behavior, 'timeStamp.rowtime as 'ts)
    val resultTable=tableEnv.sqlQuery(
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        row_number() over (partition by windowEnd order by ct desc) row_num
        |    from (
        |        select
        |            itemId,
        |            hop_end(ts,interval '5' minute,interval '1' hour) windowEnd,
        |            count(itemId) ct
        |        from agg
        |        where behavior = 'pv'
        |        group by itemId,hop(ts,interval '5' minute,interval '1' hour)
        |    )
        |)
        |where row_num<=5
      """.stripMargin)
    resultTable.toRetractStream[(Long,Timestamp,Long,Long)].print()
    env.execute("hot items job")
  }
}
