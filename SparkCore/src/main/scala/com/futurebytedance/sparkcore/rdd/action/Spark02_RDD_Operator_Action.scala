package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/24 - 23:06
 * @version 1.0
 *          RDD行动算子-reduce
 *
 * 函数签名
 * def reduce(f: (T, T) => T): T
 * 函数说明
 * 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
 */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子 - reduce
    val i: Int = rdd.reduce(_ + _)

    println(i)

    sc.stop()
  }
}
