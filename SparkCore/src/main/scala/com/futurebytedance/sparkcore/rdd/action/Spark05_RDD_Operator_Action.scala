package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:13
 * @version 1.0
 *          RDD行动算子-first
 *
 * 函数签名
 * def first(): T
 * 函数说明
 * 返回 RDD 中的第一个元素
 */
object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子 - first
    //first:获取数据源中的第一个
    val first: Int = rdd.first()
    println(first)

    sc.stop()
  }
}
