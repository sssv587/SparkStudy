package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/23 - 0:56
 * @version 1.0
 *          RDD转换算子-KV类型-各种ByKey算子的关系
 */
object Spark20_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-foldByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a", 1), ("a", 2), ("b", 3),
        ("b", 4), ("b", 5), ("a", 9)
      ), 2)


    /*
    reduceByKey:
    combineByKeyWithClassTag[V](
        (v: V) => v, //第一个值不会参与计算
        func,  //分区内计算规则
        func,  //分区间计算规则
        partitioner)

    aggregateByKey:
    combineByKeyWithClassTag[U](
        (v: V) => cleanedSeqOp(createZero(), v), //初始值和第一个key的value值进行的分区内的操作
        cleanedSeqOp,  //分区内计算规则
        combOp, //分区间计算规则
        partitioner)

    foldByKey:
    combineByKeyWithClassTag[V](
        (v: V) => cleanedFunc(createZero(), v), //初始值和第一个key的value值进行的分区内的操作
        cleanedFunc,  //分区内计算规则
        cleanedFunc,  //分区间计算规则
        partitioner)

    combineByKey:
    combineByKeyWithClassTag(
        createCombiner,  //相同key的第一条数据进行的处理
        mergeValue,  //表示分区内数据的处理函数
        mergeCombiners,  //表示分区间数据的处理函数
        defaultPartitioner(self))
     */
    rdd.reduceByKey(_ + _) //wordCount
    rdd.aggregateByKey(0)(_ + _, _ + _) //wordCount
    rdd.foldByKey(0)(_ + _) //wordCount
    rdd.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y) //wordCount

    sc.stop()
  }
}
