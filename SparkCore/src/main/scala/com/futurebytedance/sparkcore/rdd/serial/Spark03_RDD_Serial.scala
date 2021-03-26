package com.futurebytedance.sparkcore.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/27 - 0:16
 * @version 1.0
 *          RDD-序列化-Kryo序列化框架
 */
object Spark03_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SerDemo").setMaster("local[*]")
      // 替换默认的序列化机制
      .set("spark.serializer",
        "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用 kryo 序列化的自定义类
      .registerKryoClasses(Array(classOf[Searcher]))
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello scala", "scala", "haha"), 2)
    //Java 的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也
    //比较大。Spark 出于性能的考虑，Spark2.0 开始支持另外一种 Kryo 序列化机制。Kryo 速度
    //是 Serializable 的 10 倍。当 RDD 在 Shuffle 数据的时候，简单数据类型、数组和字符串类型
    //已经在 Spark 内部使用 Kryo 来序列化。
    //注意：即使使用 Kryo 序列化，也要继承 Serializable 接口。
    val searcher: Searcher = Searcher("hello")
    val result: RDD[String] = searcher.getMatchedRDD1(rdd)
    result.collect.foreach(println)

  }

  case class Searcher(query: String) {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    def getMatchedRDD1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    def getMatchedRDD2(rdd: RDD[String]): RDD[String] = {
      val q: String = query
      rdd.filter(_.contains(q))
    }
  }
}
