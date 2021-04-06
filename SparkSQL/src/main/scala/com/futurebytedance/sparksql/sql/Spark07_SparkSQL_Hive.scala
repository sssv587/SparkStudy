package com.futurebytedance.sparksql.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author yuhang.sun 2021/4/7 - 0:47
 * @version 1.0
 *          SparkSQL-访问外置Hive
 */
object Spark07_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = new SparkSession.Builder().enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._

    //1.使用SparkSQL连接外置的Hive
    //2.启动Hive的支持
    //3.增加对应的依赖关系(包含MySQL的驱动)
    spark.sql("show tables").show

    //TODO 关闭环境
    spark.close()
  }
}
