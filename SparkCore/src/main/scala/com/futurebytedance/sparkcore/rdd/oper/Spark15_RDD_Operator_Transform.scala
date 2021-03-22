package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 22:52
 * @version 1.0
 *          RDD转换算子-双值类型-reduceByKey算子
 * 函数签名
 * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
 * def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)]
 *
 * 函数说明
 * 可以将数据按照相同的 Key 对 Value 进行聚合
 */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-reduceByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    //reduceByKey:相同的key的数据进行value数据的聚合操作
    //scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    //[1,2,3]
    //[3,3]
    //[6]
    //reduceByKey中如果key的数据只有一个，是不会参与运算的
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println("x=" + x + " y=" + y)
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}
