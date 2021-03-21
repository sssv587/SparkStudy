package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 21:42
 * @version 1.0
 *          RDD转换算子-map-案例
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    //长的字符串
    //短的字符串
    val mapRDD: RDD[String] = rdd.map((line: String) => {
      val data: Array[String] = line.split(" ")
      data(6)
    })

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
