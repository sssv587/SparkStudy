package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 22:13
 * @version 1.0
 *          RDD转换算子-sortBy
 */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    //sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    //sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
    val sortRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt)

    sortRDD.saveAsTextFile("output")

    sc.stop()
  }
}
