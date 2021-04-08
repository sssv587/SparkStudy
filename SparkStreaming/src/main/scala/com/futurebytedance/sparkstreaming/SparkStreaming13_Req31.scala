package com.futurebytedance.sparkstreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author yuhang.sun 2021/4/9 - 0:25
 * @version 1.0
 */
object SparkStreaming13_Req31 {
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

    //这里涉及窗口的计算
    val reduceDS: DStream[(Long, Int)] = adClickData.map(data => {
      val ts: Long = data.ts.toLong
      val newTS: Long = ts / 10000 * 10000
      (newTS, 1)
    }).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(10))

    reduceDS.foreachRDD(
      rdd => {
        val list: ListBuffer[String] = ListBuffer[String]()
        val data: Array[(Long, Int)] = rdd.sortByKey(ascending = true).collect()
        data.foreach {

          case (time, cnt) =>
            val timeString: String = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))
            list.append(s"""{ "${timeString}":"00:10", "yval":"${cnt}" }""")
        }

        //输出文件
        val out = new PrintWriter(new FileWriter(new File("C:\\Users\\10926\\IdeaProjects\\SparkStudy\\data\\adclick\\adclick.json")))
        out.println("[" + list.mkString(",") + "]")
        out.flush()
        out.close()
      })

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
