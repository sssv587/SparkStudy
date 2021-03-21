package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 22:41
 * @version 1.0
 *          RDD转换算子-mapPartitions-案例
 */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //[1,2] [3,4]
    //[2] [4]
    val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })

    mapPartitionsRDD.collect().foreach(println)

    sc.stop()
  }
}
