package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:09
 * @version 1.0
 *          RDD行动算子-collect
 *
 * 函数签名
 * def collect(): Array[T]
 * 函数说明
 * 在驱动程序中，以数组 Array 的形式返回数据集的所有元素
 */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子 - collect
    //collect:方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    val ints: Array[Int] = rdd.collect()

    println(ints.mkString(","))

    sc.stop()
  }
}
