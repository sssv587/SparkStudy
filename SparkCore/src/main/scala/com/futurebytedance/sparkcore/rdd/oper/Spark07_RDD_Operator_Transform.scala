package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 0:52
 * @version 1.0
 *          RDD转换算子-filter
 *
 * 函数签名
 * def filter(f: T => Boolean): RDD[T]
 *
 * 函数说明
 * 将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
 * 当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。
 */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO算子-filter
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
