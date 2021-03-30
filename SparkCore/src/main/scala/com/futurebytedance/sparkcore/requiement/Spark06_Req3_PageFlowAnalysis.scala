package com.futurebytedance.sparkcore.requiement

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/30 - 20:23
 * @version 1.0
 */
object Spark06_Req3_PageFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark06_Req3_PageFlowAnalysis")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 页面单跳转换率统计
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(action => {
      val data: Array[String] = action.split("_")
      UserVisitAction(data(0),
        data(1).toLong,
        data(2),
        data(3).toLong,
        data(4),
        data(5),
        data(6).toLong,
        data(7).toLong,
        data(8),
        data(9),
        data(10),
        data(11),
        data(12).toLong)
    })
    actionDataRDD.cache()

    //TODO 对指定额页面连续跳转进行统计
    //1-2,2-3,3-4,4-5,5-6,6-7
    val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okFlowIds: List[(Long, Long)] = ids.zip(ids.tail)

    //TODO 计算分母
    val pageIdToCountMap: Map[Long, Long] = actionDataRDD.filter(
      action => ids.init.contains(action.page_id)
    ).map(action => {
      (action.page_id, 1L)
    }).reduceByKey(_ + _).collect().toMap

    //TODO 计算分子
    //根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    //分组后，根据访问时间进行排序(升序)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(iter => {
      val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
      //[1,2,3,4]
      //[1,2] [2,3] [3,4]
      //[1-2,2-3,3-4]
      //Sliding:划窗
      //[1,2,3,4]
      //[2,3,4]
      //zip:拉链
      val flowIds: List[Long] = sortList.map(_.page_id)
      val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
      //将不合法的页面跳转过滤
      pageFlowIds.filter(t => okFlowIds.contains(t)).map(t => (t, 1))
    })

    //((1,2),1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)
    //((1,2),sum)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)

    //TODO 计算单跳转换率
    //分子除以分母
    dataRDD.foreach {
      case ((page1, page2), sum) =>
        val lon: Long = pageIdToCountMap.getOrElse(page1, 0L)
        println(s"页面${page1}跳转到页面${page2}单跳转换率为:" + (sum.toDouble / lon))
    }


    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long
                            ) //城市 id
}
