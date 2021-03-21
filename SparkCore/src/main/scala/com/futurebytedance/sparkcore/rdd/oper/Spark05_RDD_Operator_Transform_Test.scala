package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 23:14
 * @version 1.0
 *          RDD转换算子-glom-案例
 * 函数签名:
 * def glom(): RDD[Array[T]]
 * 函数说明:
 * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 *
 */
object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //[1,2] [3,4]
    //2,4
    //6
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val maxRDD: RDD[Int] = glomRDD.map(array => {
      array.max
    })
    println(maxRDD.collect().sum)

    sc.stop()
  }
}
