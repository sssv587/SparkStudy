package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 1:01
 * @version 1.0
 *          RDD转换算子-sample
 *
 * 函数签名
 * def sample(
 *    withReplacement: Boolean,
 *    fraction: Double,
 *    seed: Long = Utils.random.nextLong): RDD[T]
 * 函数说明
 * 根据指定的规则从数据集中抽取数据
 */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    //sample算子需要传递三个参数
    //1.第一个参数表示，抽取数据后是否将数据返回true(放回)，false(丢弃)
    //2.第二个参数表示，
    //        如果抽取不放回的场合：数据源中每条数据被抽取的概率，基准值的概念
    //        如果抽取放回的场合：数据源中的每条数据被抽取的可能次数
    //3.第三个参数表示，抽取数据时随机算法的种子
    //                如果不传递第三个参数，那么使用的是当前系统时间

//    println(rdd.sample(
//      withReplacement = false,
//      0.4
//    ).collect().mkString(","))

        println(rdd.sample(
          withReplacement = true,
          2
        ).collect().mkString(","))

    sc.stop()
  }
}
