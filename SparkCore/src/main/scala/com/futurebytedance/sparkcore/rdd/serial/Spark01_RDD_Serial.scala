package com.futurebytedance.sparkcore.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yuhang.sun 2021/3/26 - 23:37
 * @version 1.0
 *          RDD-序列化-闭包检测
 */
object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User

    //SparkException: Task not serializable
    //NotSerializableException: com.futurebytedance.sparkcore.rdd.action.Spark13_RDD_Operator_Action$User

    //RDD算子中传递的函数是会包含闭包操作的，那么就会进行检测功能
    //闭包检测
    //从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor
    //端执行。那么在 scala 的函数式编程中，就会导致算子内经常会用到算子外的数据，这样就
    //形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给 Executor
    //端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列
    //化，这个操作我们称之为闭包检测。Scala2.12 版本后闭包编译方式发生了改变
    rdd.foreach {
      num => {
        println("age= " + (user.age + num))
      }
    }

    sc.stop()
  }

  //class User extends Serializable {
  //样例类在编译时，会自动混入序列化特质(实现可序列化接口)
  //case class User() {
  class User {
    var age: Int = 30
  }

}
