package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author yuhang.sun 2021/4/8 - 1:19
 * @version 1.0
 *          SparkStreaming-优雅的关闭-恢复数据
 */
object SparkStreaming09_Resume {
  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
      ssc.checkpoint("cp")

      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

      wordToOne.reduceByKey(_ + _).print()

      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()

    ssc.awaitTermination() //block 阻塞main线程
  }
}
