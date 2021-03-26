package com.futurebytedance.sparkcore.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/27 - 1:59
 * @version 1.0
 *          RDD-持久化
 * 2.RDD CheckPoint 检查点
 * 所谓的检查点其实就是通过将 RDD 中间结果写入磁盘
 * 由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点
 * 之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 * 对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。
 *
 */
object Spark04_RDD_Persist {
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

    //checkpoint需要落盘，需要指定检查点保存的路径
    //检查点路径保存的文件，当作业执行完成后，不会被删除
    //一般保存路径都是在分布式存储系统：HDFS
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("----------------------------------------------------------")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
