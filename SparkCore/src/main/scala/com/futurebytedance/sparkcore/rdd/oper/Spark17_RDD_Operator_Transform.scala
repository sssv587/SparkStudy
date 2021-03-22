package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 23:29
 * @version 1.0
 *          RDD转换算子-KV类型-aggregateByKey算子
 *
 * 函数签名
 * def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,
 * combOp: (U, U) => U): RDD[(K, U)]
 *
 * 函数说明
 * 将数据根据不同的规则进行分区内计算和分区间计算
 */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-aggregateByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)
    //(a,[1,2]),(a,[3,4])
    //(a,2),(a,6)

    //aggregateByKey存在函数柯里化，有两个参数列表
    //第一个参数列表:需要传递一个参数，表示为初始值
    //     主要用于当碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递两个参数:
    //     第一个参数表示分区内计算规则
    //     第二个参数表示分区间计算规则
    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    aggregateByKeyRDD.collect().foreach(println)

    //思考一个问题：分区内计算规则和分区间计算规则相同怎么办？
    //foldByKey

    sc.stop()
  }
}
