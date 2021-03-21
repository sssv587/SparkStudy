package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/21 - 23:11
 * @version 1.0
 *          RDD转换算子-groupBy
 */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-groupBy
    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"))

    //分组和分区没有必然的关系
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
