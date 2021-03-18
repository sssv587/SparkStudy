package com.futurebytedance.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/19 - 1:18
 * @version 1.0
 *          RDD分区-集合数据源-数据的分配
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //[1,2] [3,4]
    //sc.makeRDD(List(1, 2, 3, 4),2)
    //[1],[2],[3,4]
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),3)
    //[1],[2,3],[4,5]
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
