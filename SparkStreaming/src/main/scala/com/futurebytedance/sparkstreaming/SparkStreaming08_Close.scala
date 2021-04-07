package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * @author yuhang.sun 2021/4/8 - 1:00
 * @version 1.0
 *          SparkStreaming-优雅的关闭
 */
object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {
    /*
    线程的关闭：
    val thread = new Thread()
    thread.start()
    thread.stop() //强制关闭
     */

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    wordToOne.reduceByKey(_ + _).print()

    ssc.start()

    //如果想要关闭采集器，那么需要创建新的线程
    //而且需要在第三方程序中增加关闭状态
    new Thread(() => {
      //优雅的关闭
      //计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
      //Mysql:Table(stopSpark)=>Row=>data
      //Redis:Data(K-V)
      //ZK:/stopSpark
      //HDFS:/stopSpark
      //      while (true) {
      //        if (true) {
      //          //获取SparkStreaming状态
      //          val state: StreamingContextState = ssc.getState()
      //          if (state == StreamingContextState.ACTIVE) {
      //            ssc.stop(stopSparkContext = true, stopGracefully = true)
      //          }
      //        }
      //        Thread.sleep(5000)
      //      }

      //为了测试方便
      Thread.sleep(5000)
      val state: StreamingContextState = ssc.getState()
      if (state == StreamingContextState.ACTIVE) {
        ssc.stop(stopSparkContext = true, stopGracefully = true)
      }
      System.exit(0)
    }).start()

    ssc.awaitTermination() //block 阻塞main线程


  }
}
