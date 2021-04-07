package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

/**
 * @author yuhang.sun 2021/4/7 - 22:34
 * @version 1.0
 *          SparkStreaming-DStream的创建-自定义数据源
 */
object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())

    messageDS.print()

    ssc.start()

    ssc.awaitTermination()
  }

  /*
  自定义数据采集器
  1.继承Receiver，定义泛型，传递参数
  2.重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true

    override def onStart(): Unit = {
      new Thread(() => {
        while (flag) {
          val message: String = "采集的数据为:" + new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }

}
