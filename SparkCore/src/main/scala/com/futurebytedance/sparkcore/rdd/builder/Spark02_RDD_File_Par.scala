package com.futurebytedance.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/19 - 1:34
 * @version 1.0
 *          RDD分区-文件数据源-分区的设定
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //textFile可以将文件作为数据处理的数据源，默认也可以设定分区
    //minPartitions:最小分区数量
    //math.min(defaultParallelism, 2)
    //val rdd: RDD[String] = sc.textFile("data/1.txt")
    //如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    //Spark读取文件，底层其实就是Hadoop的读取方式
    //分区数量的计算方式:
    //totalSize = 7 字节
    //long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    //goalSize = 7 / 2 = 3(byte)
    //7/3 = 2...1(1.1倍) + 1 = 3(分区)
    val rdd: RDD[String] = sc.textFile("data/1.txt", 2)
    rdd.saveAsTextFile("output")

    //TODO 关闭环境
    sc.stop()
  }
}
