package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 22:10
 * @version 1.0
 *          RDD转换算子-sortBy
 *
 *  函数签名
 *  def sortBy[K](
 *  f: (T) => K,ascending: Boolean = true,
 *  numPartitions: Int = this.partitions.length)
 *  (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
 *
 *  函数说明
 *  该操作用于排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理
 *  的结果进行排序，默认为升序排列。排序后新产生的 RDD 的分区数与原 RDD 的分区数一
 *  致。中间存在 shuffle 的过程
 */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 3, 5, 4, 1), 2)

    val sortRDD: RDD[Int] = rdd.sortBy(num => num)

    sortRDD.saveAsTextFile("output")

    sc.stop()
  }
}
