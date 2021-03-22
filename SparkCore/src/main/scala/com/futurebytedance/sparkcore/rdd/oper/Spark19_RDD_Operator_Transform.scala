package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/23 - 0:23
 * @version 1.0
 *          RDD转换算子-KV类型-combineByKey算子
 *
 * 函数签名
 * def combineByKey[C](
 * createCombiner: V => C,
 * mergeValue: (C, V) => C,
 * mergeCombiners: (C, C) => C): RDD[(K, C)]
 *
 * 函数说明
 * 最通用的对 key-value 型 rdd 进行聚集操作的聚集函数（aggregation function）。类似于
 * aggregate()，combineByKey()允许用户返回值的类型与输入不一致。
 */
object Spark19_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-foldByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 9)
      ), 2)

    //combineByKey:方法需要三个参数
    //第一个参数表示:将相同key的第一个数据进行结构的转换，实现操作
    //第二个参数表示:分区内的计算规则
    //第三个参数表示:分区间的计算规则
    val cbRDD: RDD[(String, Int)] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    ).mapValues {
      case (x, y) => x / y
    }

    cbRDD.collect().foreach(println)

    sc.stop()
  }
}
