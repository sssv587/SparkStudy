package com.futurebytedance.sparkcore.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/27 - 0:40
 * @version 1.0
 *          RDD-依赖关系-宽窄依赖
 *
 * 窄依赖表示每一个父(上游)RDD的Partition最多被子（下游）RDD的一个Partition使用，窄依赖我们形象的比喻为独生子女
 * 宽依赖表示同一个父（上游）RDD的Partition被多个子（下游）RDD的Partition依赖，会引起 Shuffle，总结：宽依赖我们形象的比喻为多生
 */
object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile("data/11.txt")
    println(fileRDD.dependencies)

    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)

    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.dependencies)

    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.dependencies)

    resultRDD.collect().foreach(println)

    sc.stop()
  }
}
