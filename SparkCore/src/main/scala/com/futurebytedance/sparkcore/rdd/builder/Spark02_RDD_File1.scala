package com.futurebytedance.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/19 - 0:57
 * @version 1.0
 *          从文件中创建RDD
 */
object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //textFile:以行为单位来读取数据，读取的数据都是字符串
    //wholeTextFiles:以文件为单位来读取数据
    //读取的结果表示为元祖，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
