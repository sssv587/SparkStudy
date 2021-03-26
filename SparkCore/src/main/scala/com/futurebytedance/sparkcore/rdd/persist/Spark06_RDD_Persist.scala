package com.futurebytedance.sparkcore.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/27 - 2:14
 * @version 1.0
 *          RDD-持久化-cache(persist)与checkpoint的区别与联系
 *
 * 1)Cache缓存只是将数据保存起来，不切断血缘依赖。Checkpoint检查点切断血缘依赖。
 * 2)Cache缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint的数据通常存储在HDFS等容错、高可用的文件系统，可靠性高。
 * 3)建议对checkpoint()的RDD使用Cache缓存，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD
 *
 */
object Spark06_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    //cache：将数据临时存储在内存中进行数据重用
    //       会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
    //persist:将数据临时存储在磁盘文件中进行数据重用
    //        涉及到磁盘IO，性能较低，但是数据安全
    //        如果作业执行完毕，临时保存的数据文件就会丢失
    //checkpoint:将数据长久的保存在磁盘文件中进行数据重用
    //           涉及到磁盘IO，性能较低，但是数据安全
    //           为了保证数据安全，所以一般情况下，会独立执行操作
    //           为了能提高效率，一般情况下，是需要和cache联合使用
    //           执行过程中，会切断血缘关系。重新建立新的血缘关系。
    //           checkpoint等同于改变数据源

    val list: List[String] = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("**********************************")
      (word, 1)
    })

    //mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("----------------------------------------------------------")
    println(mapRDD.toDebugString)

    sc.stop()
  }
}
