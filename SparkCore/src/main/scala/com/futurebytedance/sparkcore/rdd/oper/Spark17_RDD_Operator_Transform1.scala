package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 23:51
 * @version 1.0
 *          RDD转换算子-KV类型-aggregateByKey算子
 */
object Spark17_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-aggregateByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 9)
      ), 2)
    //(a,[1,2]),(a,[3,4])
    //(a,2),(a,6)

    val aggregateByKeyRDD: RDD[(String, Int)] = rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    aggregateByKeyRDD.collect().foreach(println)

    sc.stop()
  }
}
