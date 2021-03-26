package com.futurebytedance.sparkcore.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun2021/3/27 - 2:06
 * @version 1.0
 *          RDD-持久化
 */
object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("**********************************")
      (word, 1)
    })

    mapRDD.cache()
    mapRDD.checkpoint()

    //cache：将数据临时存储在内存中进行数据重用
    //persist:将数据临时存储在磁盘文件中进行数据重用
    //        涉及到磁盘IO，性能较低，但是数据安全
    //        如果作业执行完毕，临时保存的数据文件就会丢失
    //checkpoint:将数据长久的保存在磁盘文件中进行数据重用
    //           涉及到磁盘IO，性能较低，但是数据安全
    //           为了保证数据安全，所以一般情况下，会独立执行操作
    //           为了能提高效率，一般情况下，是需要和cache联合使用

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("----------------------------------------------------------")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
