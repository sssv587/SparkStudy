package com.futurebytedance.sparkcore.requiement

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/30 - 17:01
 * @version 1.0
 */
object Spark02_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO：Top10热门分类
    //Q:actionRDD重复使用
    //Q:cogroup性能可能较低

    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    //2.统计品类的点击数量：（品类ID，点击数量）
    val clickActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val data: Array[String] = action.split("_")
        data(6) != "-1"
      }
    }
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(action
    => {
      val data: Array[String] = action.split("_")
      (data(6), 1)
    }
    ).reduceByKey(_ + _)

    //3.统计品类的下单数量：（品类ID，下单数量）
    val orderActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val data: Array[String] = action.split("_")
        data(8) != "null"
      }
    }
    //orderid => 1,2,3
    //[(1,1),(2,1),(3,1)]
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(action => {
      val data: Array[String] = action.split("_")
      val cid: String = data(8)
      val cids: Array[String] = cid.split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    //4.统计品类的支付数量：（品类ID，支付数数量）
    val payActionRDD: RDD[String] = actionRDD.filter {
      action => {
        val data: Array[String] = action.split("_")
        data(10) != "null"
      }
    }

    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(action => {
      val data: Array[String] = action.split("_")
      val cid: String = data(10)
      val cids: Array[String] = cid.split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    //(品类ID,点击数量) => (品类ID,(点击数量,0,0))
    //(品类ID,下单数量) => (品类ID,(0,下单数量,0))
    //                 => (品类ID,((品类ID,(点击数量,下单数量,0))
    //(品类ID,支付数量) => (品类ID,(0,0,支付数量))
    //                 => (品类ID,(点击数量,下单数量,支付数数量))
    //(品类ID,(点击数量,下单数量,支付数数量))

    //5.将品类进行排序，并且取前10名
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序:先比较第一个，在比较第二个，在比较第三个，以此类推
    //(品类ID,(点击数量,下单数量,支付数数量))
    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => (cid, (cnt, 0, 0))
    }

    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => (cid, (0, cnt, 0))
    }

    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => (cid, (0, 0, cnt))
    }

    //将三个数据源合并在一起，统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

    //6.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}
