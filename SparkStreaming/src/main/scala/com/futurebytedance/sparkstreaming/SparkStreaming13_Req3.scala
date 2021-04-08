package com.futurebytedance.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * @author yuhang.sun
 * @version 1.0
 *           SparkStreaming-生产数据到Kafka
 */
object SparkStreaming13_Req3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "streaming",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("streaming"), kafkaPara)
    )

    val adClickData: DStream[AdClickData] = kafkaDataDS.map(kafkaData => {
      val data: String = kafkaData.value()
      val datas: Array[String] = data.split(" ")
      AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //最近一分钟，每10秒计算一次
    //12:01=>12:00
    //12:11=>12:10
    //12:19=>12:10
    //12:25=>12:20
    //12:59=>12:50

    // 55 => 50, 49 => 40, 32 => 30
    // 55 / 10 * 10 => 50
    // 49 / 10 * 10 => 40
    // 32 / 10 * 10 => 30

    //这里涉及窗口的计算
    val reduceDS: DStream[(Long, Int)] = adClickData.map(data => {
      val ts: Long = data.ts.toLong
      val newTS: Long = ts / 10000 * 10000
      (newTS, 1)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(10))

    reduceDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
