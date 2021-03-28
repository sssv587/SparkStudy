package com.futurebytedance.sparkcore.rdd.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * @author yuhang.sun 2021/3/28 - 23:52
 * @version 1.0
 *          RDD-系统累加器
 */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //获取系统累加器
    //Spark默认就提供了简单数据聚合的累加器
    val sumAccumulator: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //    sc.collectionAccumulator

    rdd.foreach(num => {
      //使用累加器
      sumAccumulator.add(num)
    })

    //获取累加器的值
    println(sumAccumulator.value)

    sc.stop()
  }
}
