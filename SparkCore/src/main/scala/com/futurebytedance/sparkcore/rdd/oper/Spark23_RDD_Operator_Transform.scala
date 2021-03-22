package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/23 - 1:19
 * @version 1.0
 *          RDD转换算子-cogroup算子
 *
 * 函数签名
 * def cogroup[W](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))]
 *
 * 函数说明
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
 */
object Spark23_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-cogroup算子
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 3), ("c", 6)))

    //cogroup:connect+group(分组，连接)
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    cgRDD.collect().foreach(println)

    sc.stop()
  }
}
