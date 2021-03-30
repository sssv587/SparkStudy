package com.futurebytedance.sparkcore.framework.util

import org.apache.spark.SparkContext

/**
 * @author yuhang.sun 2021/3/30 - 22:18
 * @version 1.0
 */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]

  def put(sc: SparkContext): Unit = {
    scLocal.set(sc)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear(): Unit = {
    scLocal.remove()
  }
}
