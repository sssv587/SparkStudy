package com.futurebytedance.sparkcore.rdd.oper

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/22 - 22:35
 * @version 1.0
 *          RDD转换算子-partitionBy
 *
 * 函数签名
 * def partitionBy(partitioner: Partitioner): RDD[(K, V)]
 *
 * 函数说明
 * 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 */
object Spark14_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    //TODO 算子-key-value类型
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    //RDD => PairRDDFunctions
    //隐式转换(二次编译)
    /*
    RDD => PairRDDFunctions
    implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
    new PairRDDFunctions(rdd)
    }
     */
    //partitionBy根据指定的分区规则对数据进行重分区-改变的是分区的数据
    /*
    def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
    }
     */
    val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))

    //思考一个问题：如果重分区的分区器和当前 RDD 的分区器一样怎么办？
    /*
    if (self.partitioner == Some(partitioner)) {
      self
    } else {
      new ShuffledRDD[K, V, V](self, partitioner)
    }

    override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
    }
     */
    //类型+数量 => 不会发生改变

    //思考一个问题：Spark 还有其他分区器吗？
    //HashPartitioner、RangePartitioner(排序)

    //思考一个问题：如果想按照自己的方法进行数据分区怎么办？
    //自己写一个分区器  extends Partitioner

    newRDD.partitionBy(new HashPartitioner(2))

    newRDD.saveAsTextFile("output")

    sc.stop()
  }
}
