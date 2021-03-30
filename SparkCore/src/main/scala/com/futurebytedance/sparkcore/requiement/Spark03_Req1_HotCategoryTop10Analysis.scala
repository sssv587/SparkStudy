package com.futurebytedance.sparkcore.requiement

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/30 - 17:13
 * @version 1.0
 *
 */
object Spark03_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO：Top10热门分类
    //Q:存在大量的shuffle操作(reduceByKey)
    //reduceByKey 聚合算子，spark会提供优化，缓存

    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    //2.将数据转换结构
    //   点击的场合： (品类ID,(1,0,0))
    //   下单的场合： (品类ID,(0,1,0))
    //   支付的场合： (品类ID,(0,0,1))
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

    //4.将统计结果根据数量进行降序处理，取前10名
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, ascending = false).take(10)

    //5.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}
