package com.futurebytedance.sparkcore.requiement

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author yuhang.sun 2021/3/30 - 17:38
 * @version 1.0
 */
object Spark04_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO：Top10热门分类
    //Q:存在大量的shuffle操作(reduceByKey)
    //reduceByKey 聚合算子，spark会提供优化，缓存

    //1.读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    actionRDD.cache()

    val acc = new HotCategoryAccumulator()
    sc.register(acc, "hotCategory")

    //2.将数据转换结构
    actionRDD.foreach(action => {
      val data: Array[String] = action.split("_")
      if (data(6) != "-1") {
        //点击的场合
        acc.add((data(6), "click"))
      } else if (data(8) != null) {
        //下单的场合
        val ids: Array[String] = data(8).split(",")
        ids.foreach(id => {
          acc.add((id, "order"))
        })
      } else if (data(10) != null) {
        //支付的场合
        val ids: Array[String] = data(10).split(",")
        ids.foreach(id => {
          acc.add((id, "pay"))
        })
      }
    })

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accValue.map(_._2)
    val sort: List[HotCategory] = categories.toList.sortWith((left, right) =>
      if (left.clickCnt > right.clickCnt) {
        true
      } else if (left.clickCnt == right.clickCnt) {
        if (left.orderCnt > right.orderCnt) {
          true
        } else if (left.orderCnt == right.orderCnt) {
          if (left.payCnt > right.payCnt) {
            true
          } else {
            false
          }
        } else {
          false
        }
      }
      else {
        false
      }
    )

    //5.将结果采集到控制台打印出来
    sort.take(10).foreach(println)

    sc.stop()
  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1.继承AccumulatorV2，定义泛型
   * IN:(品类ID,行为类型)
   * OUT:mutable.Map[String,HotCategory]
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
      val map1: mutable.Map[String, HotCategory] = this.hcMap
      val map2: mutable.Map[String, HotCategory] = other.value

      map2.foreach {
        case (cid, hc) =>
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
      }
    }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }

}
