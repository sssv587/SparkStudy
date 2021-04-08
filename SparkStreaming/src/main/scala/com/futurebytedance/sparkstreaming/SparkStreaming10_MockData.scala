package com.futurebytedance.sparkstreaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @author yuhang.sun 2021/4/8 - 1:25
 * @version 1.0
 *          SparkStreaming-生产数据到Kafka
 */
object SparkStreaming10_MockData {
  def main(args: Array[String]): Unit = {
    //生成模拟数据
    //格式：timestamp area city userid adid
    //含义： 时间戳   省份  城市  用户   广告

    //Application => Kafka => SparkStreaming => Analysis
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    // kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streaming
    // zkServer
    // .\bin\windows\kafka-server-start.bat .\config\server.properties



    while (true) {
      mockData().foreach(
        data => {
          //向Kafka中生成数据
          val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("streaming", data)
          producer.send(record)
        }
      )

      Thread.sleep(2)
    }
  }

  def mockData(): ListBuffer[String] = {
    val list: ListBuffer[String] = ListBuffer[String]()
    val areaList: ListBuffer[String] = ListBuffer[String]("华北", "华东", "华南")
    val cityList: ListBuffer[String] = ListBuffer[String]("北京", "上海", "深圳")
    for (_ <- 1 to 30) {
      val area: String = areaList(new Random().nextInt(3))
      val city: String = cityList(new Random().nextInt(3))
      val userid: Int = new Random().nextInt(6) + 1
      val adid: Int = new Random().nextInt(6) + 1
      list.append(s"${System.currentTimeMillis()} $area $city $userid $adid")
    }
    list
  }
}
