package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/24 - 22:35
 * @version 1.0
 *          案例实操
 */
object Spark24_RDD_Operator_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 案例实操
    //1.获取原始数据：时间戳，省份，城市，用户，广告
    val dataRDD: RDD[String] = sc.textFile("data/agent.log")

    //2.将原始数据进行结构转换。方便统计
    //时间戳，省份，城市，用户，广告
    //=>
    //((省份,广告),1)
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(line => {
      val data: Array[String] = line.split(" ")
      ((data(1), data(4)), 1)
    })

    //3.将转换结构后的数据进行分组聚合
    //((省份,广告),1) => ((省份,广告),sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    //4.将聚合的结果进行结构的转换
    //((省份,广告),sum) => (省份,(广告,sum))
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((province, ad), sum) => (province, (ad, sum))
    }

    //5.将转换结构后的数据根据省份进行分组
    //(省份,[(广告A,sumA),(广告B,sumB)...])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    //6.将分组后的数据组内排序(降序),取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    //7.采集数据打印在控制台
    resultRDD.collect().foreach(println)

    sc.stop()
  }
}
