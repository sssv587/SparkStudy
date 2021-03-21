package com.futurebytedance.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 21:18
 * @version 1.0
 *           RDD分区-文件数据源-数据分区的分配案例
 */
object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //14 byte / 2 = 7byte
    //14/7 = 2(分区)
    /*
    1234567@@ => 012345678
    89@@ => 9101112
    0 => 13

    [0,7] => 1234567
    [7,14] => 890
     */

    //如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd: RDD[String] = sc.textFile("data/word.txt", 2)
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
