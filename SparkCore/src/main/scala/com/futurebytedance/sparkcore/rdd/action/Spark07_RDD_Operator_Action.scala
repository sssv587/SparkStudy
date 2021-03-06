package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:16
 * @version 1.0
 *          RDD行动算子-takeOrdered
 *
 * 函数签名
 * def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T]
 *
 * 函数说明
 * 返回该 RDD 排序后的前 n 个元素组成的数组
 */
object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 5, 2, 3, 4))

    //TODO - 行动算子 - takeOrdered
    //takeOrdered:数据排序后，取N个数据
    val ints: Array[Int] = rdd.takeOrdered(3)
    println(ints.mkString(","))

    sc.stop()
  }
}
