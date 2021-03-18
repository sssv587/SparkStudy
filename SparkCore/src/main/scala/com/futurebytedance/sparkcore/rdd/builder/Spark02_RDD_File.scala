package com.futurebytedance.sparkcore.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/19 - 0:49
 * @version 1.0
 *          从文件中创建RDD
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    //TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD，将文件中的数据作为处理的数据源
    //path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    //sc.textFile("C:\\Users\\10926\\IdeaProjects\\SparkStudy\\data\\1.txt")
    //val rdd: RDD[String] = sc.textFile("data/1.txt")
    //path路径可以是文件的具体路径，也可以是目录名称
    //val rdd: RDD[String] = sc.textFile("data")
    //path路径还可以使用通配符 *
    val rdd: RDD[String] = sc.textFile("data/1*.txt")
    //path还可以是分布式存储系统路径：HDFS
    //sc.textFile("hdfs://linux1:9000/test.txt")

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }
}
