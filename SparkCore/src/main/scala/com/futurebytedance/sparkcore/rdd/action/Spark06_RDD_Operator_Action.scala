package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:15
 * @version 1.0
 *          RDD行动算子-take
 *
 * 函数签名
 * def take(num: Int): Array[T]
 * 函数说明
 * 返回一个由 RDD 的前 n 个元素组成的数组
 */
object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子 - take
    //take:获取N个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    sc.stop()
  }
}
