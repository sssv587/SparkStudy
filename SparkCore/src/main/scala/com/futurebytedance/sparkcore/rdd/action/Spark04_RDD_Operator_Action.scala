package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:11
 * @version 1.0
 *          RDD行动算子-count
 *
 * 函数签名
 * def count(): Long
 * 函数说明
 * 返回 RDD 中元素的个数
 */
object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子 - count
    //count:数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    sc.stop()
  }
}
