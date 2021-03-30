package com.futurebytedance.sparkcore.framework.application

import com.futurebytedance.sparkcore.framework.common.TApplication
import com.futurebytedance.sparkcore.framework.controller.WordCountController

/**
 * @author yuhang.sun 2021/3/30 - 21:47
 * @version 1.0
 *          应用层
 */
object WordCountApplication extends App with TApplication {
  start() {
    val controller = new WordCountController()
    controller.dispatch()
  }
}
