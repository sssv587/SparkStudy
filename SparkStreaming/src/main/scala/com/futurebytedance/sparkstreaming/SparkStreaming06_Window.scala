package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author yuhang.sun 2021/4/8 - 0:24
 * @version 1.0
 */
object SparkStreaming06_Window {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //窗口的范围应该是采集周期的整数倍
    //窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
    //这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的幅度(步长)
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

    val wordToCount: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

    wordToCount.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
