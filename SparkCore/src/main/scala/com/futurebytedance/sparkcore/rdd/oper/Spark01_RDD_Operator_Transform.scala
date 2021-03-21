package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/21 - 21:34
 * @version 1.0
 *          RDD转换算子-map
 */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //1,2,3,4
    //2,4,6,8

    //转换函数
    def mapFunction(number: Int): Int = {
      number * 2
    }

    //val mapRDD: RDD[Int] = rdd.map(mapFunction)
    //val mapRDD: RDD[Int] = rdd.map((num: Int) => {num * 2})
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
