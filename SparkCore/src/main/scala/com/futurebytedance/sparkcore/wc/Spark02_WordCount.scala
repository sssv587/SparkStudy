package com.futurebytedance.sparkcore.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/16 - 22:02
 * @version 1.0
 *          Spark实现WordCount
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map {
      word => (word, 1)
    }

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

    val wordCount = wordGroup.map {
      case (_, list) =>
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }

    val array = wordCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }
}
