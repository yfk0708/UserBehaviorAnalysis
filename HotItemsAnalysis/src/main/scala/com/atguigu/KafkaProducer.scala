package com.atguigu

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Kafka生产者读取文件中数据
 */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "bigdata-pro01.kfk.com:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //    创建kafka producer
    val producer = new KafkaProducer[String, String](properties)
    //    从文件中读取数据
    val source = io.Source.fromFile("G:\\Projects\\atguigu\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for (line <- source.getLines()) {
      val record = new ProducerRecord[String, String]("hotItems", line)
      producer.send(record)
    }
    producer.close()
  }
}
