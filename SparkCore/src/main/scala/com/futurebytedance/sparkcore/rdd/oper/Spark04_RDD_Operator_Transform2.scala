package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 23:07
 * @version 1.0
 *          RDD转换算子-flatMap-案例
 */
object Spark04_RDD_Operator_Transform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-flatMap
    val rdd: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD: RDD[Any] = rdd.flatMap(data => {
      data match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
