package com.futurebytedance.sparkcore.rdd.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author yuhang.sun 2021/3/29 - 0:43
 * @version 1.0
 *          RDD-广播变量
 *
 * 广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个
 * 或多个 Spark 操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，
 * 广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark 会为每个任务
 * 分别发送。
 */
object Spark01_BroadCast {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    //join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)
    //(a,1), (b,2), (c,3)
    //(a,(1,4)) (b,(2,5)) (c,(3,6))

    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) =>
        //访问广播变量
        val l: Int = bc.value.getOrElse(w, 0)
        (w, (c, l))
    }.collect().foreach(println)

    sc.stop()
  }
}
