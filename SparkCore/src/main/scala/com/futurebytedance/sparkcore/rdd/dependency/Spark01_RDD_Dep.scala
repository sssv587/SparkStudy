package com.futurebytedance.sparkcore.rdd.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/27 - 0:32
 * @version 1.0
 *          RDD-依赖关系-血缘关系
 *
 * RDD 只支持粗粒度转换，即在大量记录上执行的单个操作。将创建 RDD 的一系列 Lineage
 * （血统）记录下来，以便恢复丢失的分区。RDD 的 Lineage 会记录 RDD 的元数据信息和转
 * 换行为，当该 RDD 的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的
 * 数据分区。
 */
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile("data/11.txt")
    println(fileRDD.toDebugString)

    println("----------------------")

    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)

    println("----------------------")

    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println(mapRDD.toDebugString)

    println("----------------------")

    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println(resultRDD.toDebugString)

    resultRDD.collect().foreach(println)

    sc.stop()
  }
}
