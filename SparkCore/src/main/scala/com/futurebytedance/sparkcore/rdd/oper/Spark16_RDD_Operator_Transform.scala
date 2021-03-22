package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 23:00
 * @version 1.0
 *          RDD转换算子-KV类型-groupByKey算子
 *
 * 函数签名
 * def groupByKey(): RDD[(K, Iterable[V])]
 * def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]
 * def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
 *
 * 函数说明
 * 将数据源的数据根据key对value进行分组
 */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-(key-value)类型-groupByKey算子
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))
    //groupByKey:将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元祖
    //           元祖中的第一个元素就是key
    //           元祖中的第二个元素就是相同key的value的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    //groupByKey:一定是按照第一个元素进行分组，聚合后的value是value的集合
    //groupBy:不确定按照哪个元素进行分组，聚合后的value是key-value的集合

    //思考一个问题：reduceByKey和groupByKey的区别？
    //从shuffle的角度：都存在shuffle的操作，但是reduceByKey会在分组前对分区内的数据进行预聚合(combine)功能，
    //这样会减少落盘的数据量；而groupByKey只是进行分组，不存在数据量减少的问题，reduceByKey性能比较高
    //从功能的角度：reduceByKey其实包含分组和聚合的功能。groupByKey只能分组，不能聚合
    //所以在分组聚合的场景下，推荐使用reduceByKey；如果仅仅是分组而不需要聚合，那么还是只能使用groupByKey
    //从结果RDD泛型来看：reduceByKey返回的是一个(K,V)，groupByKey返回的是(K,V(Iterable))

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
