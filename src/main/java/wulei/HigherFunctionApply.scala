package wulei

import scala.io.Source

object HigherFunctionApply {
  val filePath = ""

  def main(args: Array[String]): Unit = {

    //模板文件
    //用户兴趣
    /**
     * 体育-篮球-NBA-湖人
     * 军事-武器-步枪-AK47
     */
    val hi = sayHello(_)
    println(hi("teacher"))

    val hey = (name: String) => {"Hello " + name}
    println(hey("student"))
  }

  def sayHello(name : String): String = {
    "Hello " + name
  }




  /**
  实现方式1
  输入参数：模板文件路径，用户兴趣字符串
  返回值：用户兴趣字符串对应的索引值
   */

  // 函数定义
  def findIndex(templatePath: String, interest: String): Int = {
    val source = Source.fromFile(filePath, "UTF-8")
    val lines = source.getLines().toArray
    source.close()
    val searchMap = lines.zip(0 until lines.size).toMap
    searchMap.getOrElse(interest, -1)
  }

//  // Dataset中的函数调用
//  findIndex(filePath, "体育-篮球-NBA-湖人")

  /**
  实现方式2
  输入参数：模板文件路径，用户兴趣字符串
  返回值：用户兴趣字符串对应的索引值
   */
  // 函数定义
  val findIndex: (String) => (String) => Int = {
    (filePath) =>
      val source = Source.fromFile(filePath, "UTF-8")
      val lines = source.getLines().toArray
      source.close()
      val searchMap = lines.zip(0 until lines.size).toMap
      (interest) => searchMap.getOrElse(interest, -1)
  }

  /** 代码说明：
   * val findIndex: (String) => (String) => Int =
      在这段代码里面，“(String) => (String) => Int”是findIndex这个变量的类型，它的含义是：
      1）首先这个类型，指的是一个函数
      2）函数的输入参数，是一个String的变量，也就是第一个(String)
      3）而函数的输出，是另外一个函数，也就是“(String) => Int”，这个就好理解一些，这个函数输入参数是String的变量，输出是Int的变量

      所以说，findIndex是一个高阶函数，他的输入是String，而输出是一个函数。
   */

//  val partFunc = findIndex(filePath)
//
//  // Dataset中的函数调用
//  partFunc("体育-篮球-NBA-湖人")
  /**
   * 总结：使用高阶函数封装调用逻辑，充分利用 Spark 调度系统的工作原理，数据不动代码动
   * 链接：https://time.geekbang.org/column/article/355028
   */
}
