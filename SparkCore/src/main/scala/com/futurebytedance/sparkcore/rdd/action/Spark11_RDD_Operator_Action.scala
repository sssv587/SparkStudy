package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/25 - 0:39
 * @version 1.0
 *          RDD行动算子-save
 *
 *
 * 函数签名
 * def saveAsTextFile(path: String): Unit
 * def saveAsObjectFile(path: String): Unit
 * def saveAsSequenceFile(
 * path: String,
 * codec: Option[Class[_ <: CompressionCodec]] = None): Unit
 *
 * 函数说明
 * 将数据保存到不同格式的文件中
 */
object Spark11_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)), 2)

    //TODO - 行动算子 - save
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output")
    //saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd.saveAsSequenceFile("output2")

    sc.stop()
  }
}
