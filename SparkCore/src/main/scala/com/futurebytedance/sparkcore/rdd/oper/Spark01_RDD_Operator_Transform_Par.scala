package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 21:46
 * @version 1.0
 *          RDD转换算子-map-并行计算
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-map

    //1.rdd的计算一个分区内的数据是一个一个执行逻辑
    //  只有前面一个数据的全部逻辑执行完毕后，才会执行下一个数据。
    //  分区内数据的执行是有序的。
    //2.不同的分区数据计算是无序的
    //val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 1)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[Int] = rdd.map(num => {
      println(">>>>>" + num)
      num
    })

    val mapRDD1: RDD[Int] = mapRDD.map(num => {
      println("######" + num)
      num
    })

    mapRDD1.collect()

    sc.stop()
  }
}
