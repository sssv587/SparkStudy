package com.futurebytedance.sparkstreaming

import com.futurebytedance.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author yuhang.sun 2021/4/8 - 23:09
 * @version 1.0
 *          SparkStreaming-广告点击量实时统计
 */
object SparkStreaming12_Req2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

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

    val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
      data => {
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val day: String = sdf.format(new Date(data.ts.toLong))
        val area: String = data.area
        val city: String = data.city
        val ad: String = data.ad

        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn: Connection = JDBCUtil.getConnection
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt,area,city,adid,count)
                |values (?,?,?,?,?)
                |on DUPLICATE KEY
                |UPDATE count = count + ?
                |""".stripMargin)

            iter.foreach {
              case ((day, area, city, ad), sum) =>
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, ad)
                pstat.setInt(5, sum)
                pstat.setInt(6, sum)
                pstat.executeUpdate()
            }
            pstat.close()
            conn.close()
          })
      })

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
