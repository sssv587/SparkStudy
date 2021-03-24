package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:25
 * @version 1.0
 *          RDD行动算子-fold
 *
 * 函数签名
 * def fold(zeroValue: T)(op: (T, T) => T): T
 * 函数说明
 * 折叠操作，aggregate 的简化版操作
 */
object Spark09_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO - 行动算子 - fold

    val result: Int = rdd.fold(10)(_ + _)

    println(result)

    sc.stop()
  }
}
