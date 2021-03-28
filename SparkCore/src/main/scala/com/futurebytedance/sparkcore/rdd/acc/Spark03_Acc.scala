package com.futurebytedance.sparkcore.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @author yuhang.sun 2021/3/28 - 23:55
 * @version 1.0
 *          RDD-系统累加器
 */
object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val sumAccumulator: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //    sc.collectionAccumulator

    val mapRDD: RDD[Unit] = rdd.map(num => {
      //使用累加器
      sumAccumulator.add(num)
    })

    //获取累加器值
    //少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    //多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    //一般情况下，累加器会放置在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAccumulator.value)

    sc.stop()
  }
}
