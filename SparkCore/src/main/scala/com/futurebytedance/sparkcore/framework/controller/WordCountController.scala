package com.futurebytedance.sparkcore.framework.controller

import com.futurebytedance.sparkcore.framework.common.TController
import com.futurebytedance.sparkcore.framework.service.WordCountService

/**
 * @author yuhang.sun 2021/3/30 - 21:48
 * @version 1.0
 *          控制层/调度层
 */
class WordCountController extends TController {
  private val wordCountService = new WordCountService()

  //调度
  override def dispatch(): Unit = {
    //TODO 执行业务操作
    val array: Array[(String, Int)] = wordCountService.dataAnalysis()

    array.foreach(println)
  }
}
