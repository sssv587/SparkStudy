package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 22:41
 * @version 1.0
 *          RDD转换算子-mapPartitions-案例
 */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //[1,2] [3,4]
    //[2] [4]
    val mapPartitionsRDD: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })

    mapPartitionsRDD.collect().foreach(println)

    //map 和 mapPartitions 的区别？
    //1.数据处理角度
    //map算子是分区内一个数据一个数据的执行，类似于串行操作。而mapPartitions算子是以分区为单位进行批处理操作
    //2.功能的角度
    //map算子的主要目的是将数据源中的数据进行转换和改变。但是不会减少或增多数据
    //mapPartitions算子需要传递一个迭代器，返回一个迭代器，没有要求元素的个数保持不变，所以可以增加或减少数据
    //3.性能的角度
    //map算子因为类似于串行操作，所以性能比较低，而mapPartitions算子类似于批处理，性能较高
    //但是mapPartitions算子会长时间占用内存，那么这样会导致内存可能不够用，出现内存溢出的错误。所以在内存有限的情况下，不推荐使用。


    sc.stop()
  }
}
