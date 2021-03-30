package com.futurebytedance.sparkcore.framework.service

import com.futurebytedance.sparkcore.framework.common.TService
import com.futurebytedance.sparkcore.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/30 - 21:48
 * @version 1.0
 *          服务层
 */
class WordCountService extends TService {
  private val wordCountDao = new WordCountDao

  //数据分析
  override def dataAnalysis(): Array[(String, Int)] = {
    val lines: RDD[String] = wordCountDao.readFile("data/11.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map {
      word => (word, 1)
    }

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

    val wordCount: RDD[(String, Int)] = wordGroup.map {
      case (_, list) =>
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }

    val array: Array[(String, Int)] = wordCount.collect()
    array
  }
}
