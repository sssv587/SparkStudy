package com.futurebytedance.sparksql.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, TypedColumn}

/**
 * @author yuhang.sun 2021/4/7 - 0:28
 * @version 1.0
 *          SparkSQL-读取并存储到MySQL数据库
 */
object Spark06_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //读取MySQL数据
    //方法一：通用的 load 方法读取
    val df: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/python")
      .option("driver", "com.mysql.jdbc.Driver").option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load()
    df.show

    //方式二:通用的load方法读取参数另一种形式
//    spark.read.format("jdbc").options(Map("url"->"jdbc:mysql://linux1:3306/spark-sql?user=root&password=123123","dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show

    //方式三:使用jdbc方法读取
//    val props: Properties = new Properties()
//    props.setProperty("user", "root")
//    props.setProperty("password", "123123")
//    val df: DataFrame = spark.read.jdbc("jdbc:mysql://linux1:3306/spark-sql","user", props)
//    df.show

    //保存数据
    //方式一:通用的方式 format 指定写出类型
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/python")
      .option("driver", "com.mysql.jdbc.Driver").option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user1").mode(SaveMode.Append).save()

    //方式二:通过 jdbc 方法
    //val props: Properties = new Properties()
    //props.setProperty("user", "root")
    //props.setProperty("password", "123123")
    //ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://linux1:3306/spark-sql",
    //"user", props)

    //TODO 关闭环境
    spark.close()
  }
}
