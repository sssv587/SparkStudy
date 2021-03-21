package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 1:27
 * @version 1.0
 *          RDD转换算子-coalesce
 *
 * 函数签名
 * def coalesce(numPartitions: Int, shuffle: Boolean = false,
 * partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
 * (implicit ord: Ordering[T] = null)
 * : RDD[T]
 *
 * 函数说明
 * 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 * 当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减小任务调度成本
 */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-coalesce
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    //coalesce方法默认情况下不会将分区的数据打乱重新组合
    //这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    //如果想要让数据均衡，可以进行shuffle处理
    //val newRDD: RDD[Int] = rdd.coalesce(2)
    val newRDD: RDD[Int] = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
