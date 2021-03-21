package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 22:35
 * @version 1.0
 *          RDD转换算子-mapPartitions
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //mapPartitions:可以以分区为单位进行数据转换操作
    //              但是会将整个分区的数据加载到内存进行引用
    //              如果处理完的数据是不会被释放掉，存在对象的引用
    //              在内存较小，数据量较大的场合下，容易出现内存溢出
    val mapPartitionsRdd: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>>>>")
      iter.map(_ * 2)
    })

    mapPartitionsRdd.collect().foreach(println)

    sc.stop()
  }
}
