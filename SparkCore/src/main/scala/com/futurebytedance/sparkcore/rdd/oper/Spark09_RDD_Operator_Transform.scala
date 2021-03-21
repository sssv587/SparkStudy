package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 1:18
 * @version 1.0
 *          RDD转换算子-distinct
 *
 * 函数签名
 * def distinct()(implicit ord: Ordering[T] = null): RDD[T]
 * def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T]
 *
 * 函数说明
 * 将数据集中重复的数据去重
 */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-distinct
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    //map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    //(1,null),(2,null),(3,null),(4,null),(1,null),(2,null),(3,null),(4,null)
    //(1,null),(1,null)
    //(null,null) => null
    //(1,null)
    val distinctRDD: RDD[Int] = rdd.distinct()

    distinctRDD.collect().foreach(println)

    sc.stop()
  }
}
