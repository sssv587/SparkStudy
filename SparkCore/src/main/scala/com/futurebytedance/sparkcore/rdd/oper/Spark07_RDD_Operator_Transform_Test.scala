package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 0:59
 * @version 1.0
 *          RDD转换算子-filter
 */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO算子-filter
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    rdd.filter((line: String) => {
      val data: Array[String] = line.split(" ")
      val time: String = data(3)
      time.startsWith("17/05/2015")
    }).collect().foreach(println)

    sc.stop()
  }
}
