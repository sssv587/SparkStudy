package com.futurebytedance.sparkcore.rdd.oper

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 0:43
 * @version 1.0
 *          RDD转换算子-groupBy
 */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-groupBy
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map((line: String) => {
      val data: Array[String] = line.split(" ")
      val time: String = data(3)
      //time.substring(0,)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = sdf.parse(time)
      val sdf1: SimpleDateFormat = new SimpleDateFormat("HH")
      val hour: String = sdf1.format(date)
      (hour, 1)
    }).groupBy((_: (String, Int))._1)

    timeRDD.map {
      case (hour, iter) => (hour, iter.size)
    }.collect().foreach(println)

    sc.stop()
  }
}
