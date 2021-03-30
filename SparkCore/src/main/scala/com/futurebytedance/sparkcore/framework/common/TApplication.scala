package com.futurebytedance.sparkcore.framework.common

import com.futurebytedance.sparkcore.framework.util.EnvUtil
import jdk.internal.util.EnvUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/30 - 22:04
 * @version 1.0
 */
trait TApplication {
  //TODO 建立和Spark框架的连接
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc: SparkContext = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex: Exception => println(ex.getMessage)
    }

    //TODO 关闭连接
    EnvUtil.clear()
    sc.stop()
  }
}
