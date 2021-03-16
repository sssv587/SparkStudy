package com.futurebytedance.sparkcore.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/16 - 22:14
 * @version 1.0
 *          Spark实现WordCount
 */
object Spark03_WordCount {
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

    //Spark框架提供了更多地框架，可以将分组和聚合使用一个方法实现
    //reduceByKey:相同的key的数据，可以对value进行reduce聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)
  }
}
