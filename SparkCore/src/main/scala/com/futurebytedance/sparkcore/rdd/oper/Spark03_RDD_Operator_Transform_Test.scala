package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/21 - 22:53
 * @version 1.0
 *          RDD转换算子-mapPartitionsWithIndex-案例
 */
object Spark03_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //[1,2] [3,4]
    val mpiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        //1 2 3 4
        //(0,1) (2,2) (4,3),(6,4)
        iter.map(
          num => {
            (index, num)
          })
      })

    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
