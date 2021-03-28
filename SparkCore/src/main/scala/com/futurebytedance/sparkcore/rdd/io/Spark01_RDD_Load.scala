package com.futurebytedance.sparkcore.rdd.io

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/28 - 23:34
 * @version 1.0
 *          RDD-文件的读取与加载
 */
object Spark01_RDD_Load {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output2")
    println(rdd1.collect().mkString(","))

    val rdd2: RDD[(String,Int)] = sc.sequenceFile[String,Int]("output3")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
