package com.futurebytedance.sparkcore.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/27 - 1:35
 * @version 1.0
 *          RDD-持久化
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("----------------------------------------------------------")

    val list1: List[String] = List("Hello Scala", "Hello Spark")

    val rdd1: RDD[String] = sc.makeRDD(list1)

    val flatRDD1: RDD[String] = rdd1.flatMap(_.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
