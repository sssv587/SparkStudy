package com.futurebytedance.sparkcore.rdd.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/28 - 23:39
 * @version 1.0
 *          RDD-累加器
 * 累加器用来把 Executor 端变量信息聚合到 Driver 端。在 Driver 程序中定义的变量，在
 * Executor 端的每个 Task 都会得到这个变量的一份新的副本，每个 task 更新这些副本的值后，
 * 传回 Driver 端进行 merge。
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //reduce:分区内、分区间
    //    val i: Int = rdd.reduce(_ + _)
    //    println(i)

    var sum = 0
    rdd.foreach(num => {
      sum += num
    })

    println("sum=" + sum)

    sc.stop()
  }
}
