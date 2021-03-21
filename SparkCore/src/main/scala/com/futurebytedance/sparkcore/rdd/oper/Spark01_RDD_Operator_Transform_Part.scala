package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/21 - 21:46
 * @version 1.0
 *          RDD转换算子-map-并行计算
 */
object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    rdd.saveAsTextFile("output")

    //[1,2],[3,4]
    //[2,4],[6,8]
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.saveAsTextFile("output1")

    sc.stop()
  }
}
