package com.futurebytedance.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author yuhang.sun 2021/4/6 - 22:40
 * @version 1.0
 *          SparkSQL-UDF函数
 */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("data/user.json")
    df.createTempView("user")

    spark.udf.register("prefixName", (name: String) => "Name:" + name)

    spark.sql("select age,prefixName(username) as `newName` from user").show

    //TODO 关闭环境
    spark.close()
  }
}
