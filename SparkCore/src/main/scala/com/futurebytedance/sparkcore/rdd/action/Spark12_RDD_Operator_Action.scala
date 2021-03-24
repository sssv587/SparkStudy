package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/25 - 0:46
 * @version 1.0
 *          RDD行动算子-foreach
 *
 * 函数签名
 * def foreach(f: T => Unit): Unit = withScope {
 * val cleanF = sc.clean(f)
 * sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
 * }
 * 函数说明
 * 分布式遍历 RDD 中的每一个元素，调用指定函数
 */
object Spark12_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    //TODO - 行动算子 - foreach
    //foreach其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)
    println("==============================")
    //foreach其实是Executor端内存数据打印
    //分布式打印
    rdd.foreach(println)

    //算子：Operator(操作)
    //     RDD的方法和集合对象的方法不一样
    //     集合对象的方法都是在同一个节点的内存中完成
    //     RDD的方法可以将计算逻辑发送到Executor(分布式节点)执行
    //     为了区分不同的处理效果，所以将RDD的方法称之为算子。
    //     RDD的方法外部的操作都是在Driver端执行，而方法内部的逻辑代码是在Executor端执行

    sc.stop()
  }
}
