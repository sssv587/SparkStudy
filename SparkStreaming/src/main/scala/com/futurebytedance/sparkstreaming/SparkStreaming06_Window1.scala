package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author yuhang.sun 2021/4/8 - 0:37
 * @version 1.0
 *          SparkStreaming-有状态-window
 */
object SparkStreaming06_Window1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //reduceByKeyAndWindow：当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
    //无需重复计算，提升性能
    val windowDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y,
      Seconds(9), Seconds(3))

    windowDS.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
