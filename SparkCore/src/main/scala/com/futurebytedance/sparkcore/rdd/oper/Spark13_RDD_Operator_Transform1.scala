package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 22:30
 * @version 1.0
 *          RDD转换算子-双值类型
 */
object Spark13_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-双value类型
    //Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //两个数据源要求分区数量要保持一致
    //Can only zip RDDs with same number of elements in each partition
    //两个数据源要求分区中数据数量保持一致

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()
  }
}
