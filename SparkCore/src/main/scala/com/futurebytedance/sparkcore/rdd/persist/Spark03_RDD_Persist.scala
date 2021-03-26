package com.futurebytedance.sparkcore.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author yuhang.sun 2021/3/27 - 1:50
 * @version 1.0
 *          RDD-持久化
 *
 * 1.RDD Cache 缓存
 * RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存
 * 在JVM的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的action算子时，该RDD将会被缓存在计算节点的内存中，并供后面重用。
 *
 * 缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD的缓存容错机
 * 制保证了即使缓存丢失也能保证计算的正确执行。通过基于RDD的一系列转换，丢失的数
 * 据会被重算，由于RDD的各个Partition 是相对独立的，因此只需要计算丢失的部分即可，
 * 并不需要重算全部 Partition。
 *
 * Spark 会自动对一些 Shuffle操作的中间数据做持久化操作(比如：reduceByKey)。这样
 * 做的目的是为了当一个节点Shuffle失败了避免重新计算整个输入。但是，在实际使用的时
 * 候，如果想重用数据，仍然建议调用persist或cache。
 *
 *
 */
object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc: SparkContext = new SparkContext(sparkConf)

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("**********************************")
      (word, 1)
    })

    //cache默认持久化的操作，只能将数据保存到内存中。如果想要保存到磁盘文件，需要更改存储级别
    //mapRDD.cache()

    //持久化操作必须在行动算子执行时完成的。
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("----------------------------------------------------------")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
