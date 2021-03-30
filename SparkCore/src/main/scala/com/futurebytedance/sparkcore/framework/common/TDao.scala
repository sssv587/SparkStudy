package com.futurebytedance.sparkcore.framework.common

import com.futurebytedance.sparkcore.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author yuhang.sun 2021/3/30 - 22:12
 * @version 1.0
 */
trait TDao {
  def readFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
