package com.futurebytedance.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yuhang.sun 2021/4/7 - 23:05
 * @version 1.0
 *          SparkStreaming-状态操作
 */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    //无状态数据操作，只对当前的采集周期内的数据进行处理
    //在某些场合下，需要保留数据统计结果(状态)，实现数据的汇总
    //使用有状态时，需要设置检查点路径
    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = data.map((_, 1))

    //val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    //updateStateByKey:根据key对数据的状态进行更新
    //传递的参数中有两个值
    //第一个值表示相同的key的value数据
    //第二个值表示缓存区相同的key的value数据
    val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        val newCount: Int = buffer.getOrElse(0) + seq.sum
        Option(newCount)
      })

    state.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
