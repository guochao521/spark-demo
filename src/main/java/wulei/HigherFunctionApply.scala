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

//  val partFunc = findIndex(filePath)
//
//  // Dataset中的函数调用
//  partFunc("体育-篮球-NBA-湖人")
  /**
   * 总结：使用高阶函数封装调用逻辑，充分利用 Spark 调度系统的工作原理，数据不动代码动
   * 链接：https://time.geekbang.org/column/article/355028
   */
}
