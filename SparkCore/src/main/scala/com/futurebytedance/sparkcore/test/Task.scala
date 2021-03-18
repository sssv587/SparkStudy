package com.futurebytedance.sparkcore.test

/**
 * @author yuhang.sun 2021/3/18 - 22:16
 * @version 1.0
 */
class Task extends Serializable {
  val data = List(1, 2, 3, 4)

  //val logic = (num:Int) => {num * 2}
  val logic: Int => Int = _ * 2

}
