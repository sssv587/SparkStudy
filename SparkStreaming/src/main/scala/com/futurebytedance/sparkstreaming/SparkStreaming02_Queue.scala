package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}

import scala.collection.mutable

/**
 * @author yuhang.sun 2021/4/7 - 22:29
 * @version 1.0
 *          SparkStreaming-DStream的创建-Queue
 */
object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时，需要传递两个参数
    //第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    //第二个参数表示批量处理的周期(采集周期)
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    val mappedStream: DStream[(Int, Int)] = inputStream.map((_, 1))

    val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)

    reducedStream.print()

    ssc.start()

    for (_ <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
