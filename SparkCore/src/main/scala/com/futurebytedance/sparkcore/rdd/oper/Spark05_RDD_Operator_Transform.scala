package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 23:11
 * @version 1.0
 *          RDD转换算子-glom
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-glom
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //List => Int
    //Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop()
  }
}
