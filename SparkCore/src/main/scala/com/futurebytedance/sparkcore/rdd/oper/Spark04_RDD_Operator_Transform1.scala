package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 23:02
 * @version 1.0
 *          RDD转换算子-flatMap
 */
object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-flatMap
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    val flatRDD: RDD[String] = rdd.flatMap(s => s.split(" "))

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
