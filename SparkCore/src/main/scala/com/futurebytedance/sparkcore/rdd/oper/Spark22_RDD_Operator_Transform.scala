package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/23 - 1:15
 * @version 1.0
 *          RDD转换算子-leftOuterJoin算子和rightOuterJoin
 *
 * 函数签名
 * def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
 * 函数说明
 * 类似于 SQL 语句的左外连接
 */
object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-leftOuterJoin算子和rightOuterJoin算子
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 3)))


    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    leftJoinRDD.collect().foreach(println)
    rightJoinRDD.collect().foreach(println)

    sc.stop()
  }
}
