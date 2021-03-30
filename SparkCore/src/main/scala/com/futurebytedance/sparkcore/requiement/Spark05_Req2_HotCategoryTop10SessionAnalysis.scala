package com.futurebytedance.sparkcore.requiement

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/30 - 20:08
 * @version 1.0
 *
 */
object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO：在需求一的基础上，增加每个品类用户 session 的点击统计

    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    val top10Ids: Array[String] = top10Category(actionRDD)

    //1.过滤原始数据，保留点击和前10品类ID
    val filterRDD: RDD[String] = actionRDD.filter(action => {
      val data: Array[String] = action.split("_")
      if (data(6) != "-1") {
        top10Ids.contains(data(6))
      } else {
        false
      }
    })

    //2.根据品类ID和seesionId进行点击量的统计
    val reduceRDD: RDD[((String, String), Int)] = filterRDD.map(action => {
      val data: Array[String] = action.split("_")
      ((data(6), data(2)), 1)
    }).reduceByKey(_ + _)

    //3.将统计的结果进行结构的转换
    //((品类ID,sessionID),sum) => (品类ID,(sessionID,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }

    //4.相同的品类进行分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //5.将分组后的数据进行点击量排序，取前十名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
    })

    resultRDD.collect().foreach(println)

    sc.stop()
  }

  def top10Category(actionRDD: RDD[String]): Array[String] = {
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(action => {
      val data: Array[String] = action.split("_")
      if (data(6) != "-1") {
        //点击的场合
        List((data(6), (1, 0, 0)))
      } else if (data(8) != null) {
        //下单的场合
        val ids: Array[String] = data(8).split(",")
        ids.map(id => (id, (0, 1, 0)))
      } else if (data(10) != null) {
        //支付的场合
        val ids: Array[String] = data(10).split(",")
        ids.map(id => (id, (0, 0, 1)))
      } else {
        Nil
      }
    })

    //3.将相同的品类ID的数据进行分组聚合
    //  (品类ID,(点击数量,下单数量,支付数量))
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })

    analysisRDD.sortBy(_._2, ascending = false).take(10).map(_._1)
  }
}
