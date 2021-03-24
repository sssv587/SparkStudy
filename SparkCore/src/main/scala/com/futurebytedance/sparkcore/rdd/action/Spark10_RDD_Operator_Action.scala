package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:27
 * @version 1.0
 *          RDD行动算子-countByKey
 */
object Spark10_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)), 2)

    //TODO - 行动算子 - countByValue,countByKey
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //println(intToLong)

    sc.stop()
  }
}
