package com.futurebytedance.sparkcore.test

/**
 * @author yuhang.sun 2021/3/18 - 22:28
 * @version 1.0
 */
class SubTask extends Serializable {
  var data: List[Int] = _
  var logic: Int => Int = _

  //计算
  def compute(): List[Int] = {
    data.map(logic)
  }
}
