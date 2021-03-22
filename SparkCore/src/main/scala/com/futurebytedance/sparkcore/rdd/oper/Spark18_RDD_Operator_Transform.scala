package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 23:59
 * @version 1.0
 *          RDD转换算子-KV类型-foldByKey算子
 *
 * 函数签名
 * def foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
 *
 * 函数说明
 * 当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey
 */
object Spark18_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-foldByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 9)
      ), 2)

    val foldKeyRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
    foldKeyRDD.collect().foreach(println)

    sc.stop()
  }
}
