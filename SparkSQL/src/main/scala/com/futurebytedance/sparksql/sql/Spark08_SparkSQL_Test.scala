package com.futurebytedance.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author yuhang.sun 2021/4/7 - 0:57
 * @version 1.0
 *          SparkSQL-案例练习
 */
object Spark08_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = new SparkSession.Builder().enableHiveSupport().config(sparkConf).getOrCreate()

    //准备数据
    //    spark.sql(
    //      """
    //        |CREATE TABLE `user_visit_action`(
    //        | `date` string,
    //        | `user_id` bigint,
    //        | `session_id` string,
    //        | `page_id` bigint,
    //        | `action_time` string,
    //        | `search_keyword` string,
    //        | `click_category_id` bigint,
    //        | `click_product_id` bigint,
    //        | `order_category_ids` string,
    //        | `order_product_ids` string,
    //        | `pay_category_ids` string,
    //        | `pay_product_ids` string,
    //        | `city_id` bigint)
    //        |row format delimited fields terminated by '\t';
    //        |""".stripMargin)
    //
    //    spark.sql(
    //      """
    //        |load data local inpath 'data/user_visit_action.txt' into table
    //        |user_visit_action;
    //        |""".stripMargin)
    //
    //    spark.sql(
    //      """CREATE TABLE `product_info`(
    //        | `product_id` bigint,
    //        | `product_name` string,
    //        | `extend_info` string)
    //        |row format delimited fields terminated by '\t';""".stripMargin)
    //    spark.sql("""load data local inpath 'data/product_info.txt' into table product_info;""")
    //    spark.sql(
    //      """CREATE TABLE `city_info`(
    //        | `city_id` bigint,
    //        | `city_name` string,
    //        | `area` string)
    //        |row format delimited fields terminated by '\t';""".stripMargin)
    //    spark.sql("""load data local inpath 'data/city_info.txt' into table city_info;""")

    //查询基本数据
    spark.sql(
      """select
        | a.*,
        |	p.product_name,
        |	c.area,
        |	c.city_name
        |from user_visit_action a
        |join product_info p on a.click_product_id = p.product_id
        |join city_info c on a.city_id = c.city_id
        |where a.click_product_id > -1""".stripMargin).createOrReplaceTempView("t1")

    //根据区域，商品进行数据聚合
    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """select
        |area,
        |product_name,
        |count(*) as clickCnt,
        |cityRemark(city_name) as city_remark
        |from
        |t1
        |group by area,product_name""".stripMargin).createOrReplaceTempView("t2")

    //区域内对点击数量进行排行
    spark.sql(
      """select
        |   *,
        |   rank() over(partition by area order by clickCnt desc) as rk
        |from
        | t2 """.stripMargin).createOrReplaceTempView("t3")


    //取前3名
    spark.sql(
      """select
        |  *
        |from
        |t3 where rk <= 3""".stripMargin).show(false)


    //TODO 关闭环境
    spark.close()
  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  //自定义聚合函数:实现城市备注功能
  //1.继承Aggregator
  // IN:城市名称
  // BUF:Buffer [总点击数量,Map[(city,count),(city,count)]]
  // OUT:备注信息
  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    //缓冲区初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    //更新缓冲区
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCount: Long = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCount)
      buff
    }

    //合并缓冲区数据
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total += buff2.total

      val map1: mutable.Map[String, Long] = buff1.cityMap
      val map2: mutable.Map[String, Long] = buff2.cityMap

      //两个Map的合并操作
      //      buff1.cityMap = map1.foldLeft(map2) {
      //        case (map, (city, cnt)) =>
      //          val newCount: Long = map.getOrElse(city, 0L) + cnt
      //          map.update(city, newCount)
      //          map
      //      }

      map2.foreach {
        case (city, cnt) =>
          val newCount: Long = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCount)
      }
      buff1.cityMap = map1
      buff1
    }

    //将统计的结果生成字符串信息
    override def finish(buff: Buffer): String = {
      val remarkList: ListBuffer[String] = ListBuffer[String]()
      val totalCnt: Long = buff.total
      val cityMap: mutable.Map[String, Long] = buff.cityMap
      //降序排列
      val cityCountList: List[(String, Long)] = cityMap.toList.sortWith((left, right) => left._2 > right._2).take(2)

      val hasMore: Boolean = cityMap.size > 2
      var rSum = 0L

      cityCountList.foreach {
        case (city, cnt) =>
          val r: Long = cnt * 100 / totalCnt
          remarkList.append(s"$city $r%")
          rSum += r
      }
      if (hasMore) {
        remarkList.append(s"其他 ${100 - rSum}%")
      }

      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
