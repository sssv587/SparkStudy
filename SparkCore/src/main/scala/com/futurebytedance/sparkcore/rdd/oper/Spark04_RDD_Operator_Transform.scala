package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 22:57
 * @version 1.0
 *          RDD转换算子-flatMap
 */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-flatMap
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))

    val flatRDD: RDD[Int] = rdd.flatMap(list => list)

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
