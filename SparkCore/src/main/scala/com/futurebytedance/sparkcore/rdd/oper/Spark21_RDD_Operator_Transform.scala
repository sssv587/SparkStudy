package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/23 - 1:07
 * @version 1.0
 *          RDD转换算子-join算子
 *
 * 函数签名
 * def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
 *
 * 函数说明
 * 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的
 * (K,(V,W))的 RDD
 */
object Spark21_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-join算子
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 2)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    //join:两个不同的数据源的数据，相同的key的value会连接在一起，形成元祖
    //     如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    //     如果两个数据源中key有多个相同，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    joinRDD.collect().foreach(println)

    //思考一个问题：如果 key 存在不相等呢？

    sc.stop()
  }
}
