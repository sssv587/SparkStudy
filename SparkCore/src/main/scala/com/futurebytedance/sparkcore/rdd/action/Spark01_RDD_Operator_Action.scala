package com.futurebytedance.sparkcore.rdd.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/24 - 22:58
 * @version 1.0
 *          RDD行动算子
 */
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO - 行动算子
    //所谓的行动算子，其实就是触发作业（Job）执行的方法
    //底层代码调用的是环境对象的runJob方法
    //底层代码中会创建ActiveJob，并提交执行
    rdd.collect()

    sc.stop()
  }
}
