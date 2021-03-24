package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/24 - 23:20
 * @version 1.0
 *          RDD行动算子-aggregate
 *
 * 函数签名
 * def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U
 * 函数说明
 * 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
 */
object Spark08_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //TODO - 行动算子 - aggregate

    //10 + 13 + 17 = 40
    //aggregateByKey:初始值只会参与分区内计算
    //aggregate:初始值会参与分区内计算并且会参与分区间的计算
    val result: Int = rdd.aggregate(10)(_ + _, _ + _)

    println(result)

    sc.stop()
  }
}
