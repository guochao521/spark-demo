package customize.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
 * @author wangguochao
 * @date 2023/2/23 13:25
 * @Description 分析CDN日志统计出PV、UV、IP地址
 */
// 参考链接：https://www.lilinchao.com/archives/1611.html
object CDNLogAnalysis extends java.io.Serializable {

  // 匹配IP规则
  val IPPattern: Regex =   "((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))".r
  // 匹配.mp4的视频规则
  val VideoPattern: Regex = "([0-9]+).mp4".r
  // 匹配http响应码和请求大小
  val httpSizePattern: Regex = ".*\\s(200|206|304)\\s([0-9]+)\\s.*".r
  // [15/Feb/2017:00:00:46 +0800] 匹配2017：00:00:46
  val timePattern: Regex = ".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CDNLogAnalysis").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val basePath = "./src/main/resources/spark_core/"
    // 加载数据
    val input = sc.textFile(basePath + "cdn.txt")

    // 1.独立 IP 统计
//    isStatic(input)

    // 2.每个视频对应的IP数
//    videoIpStatic(input)

    // 3.统计一天中每个小时的流量
    flowOfHour(input)

    sc.stop()
  }

  /**
   * 统计独立IP数
   * @param data
   */
  def isStatic(data: RDD[String]): Unit = {
    // 1.统计独立IP
    val ipNums = data.map(line => (IPPattern.findFirstIn(line).get, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
    // 2.获取到Top10访问次数
    ipNums.take(10).foreach(println)
    // 3.获取独立IP数
    println("独立IP数：" + ipNums.count())
  }

  def videoIpStatic(data: RDD[String]): Unit = {
    // 1.获取到访问视频的行
    // 定义成函数需要注意序列化问题：https://mangocool.com/detail_1_1439971291385.html
//    def getFileNameAndIp(line: String): (String, String) = (VideoPattern.findFirstIn(line).mkString, IPPattern.findFirstIn(line).mkString)
    val getFileNameAndIp = (line: String) => (VideoPattern.findFirstIn(line).mkString, IPPattern.findFirstIn(line).mkString)
    // 2.统计每个视频的独立IP数
    val dataRDD = data
      .filter(_.matches(".*([0-9]+)\\.mp4.*"))
      .map(getFileNameAndIp)
      .groupByKey()
      .map(fileIp => (fileIp._1, fileIp._2.toList.distinct))
      .sortBy(_._2.size, false)

    // 3.输出结果
    dataRDD.foreach(t => println("视频：" + t._1 +"，独立ip数：" + t._2.size))
  }

  def flowOfHour(data: RDD[String]): Unit = {

    /**
     * 统计一天中每个小时的流量
     */
    val resultRDD = data.filter(isMatch(httpSizePattern, _))
      .filter(isMatch(timePattern, _))
      .map(getTimeAndSize)
      .groupByKey()
      .map(hourSize => (hourSize._1, hourSize._2.sum))
      .sortByKey(true,1)

    resultRDD.foreach(hour_size =>
      println(hour_size._1 + "时 CDN流量 = " + hour_size._2 / (1024 * 1024 * 1024) + "GB"))
  }

  /**
   * 使用str匹配pattern
   */
  def isMatch(pattern: Regex, str: String): Boolean= {
    str match {
      case pattern(_*) => true
      case _ => false
    }
  }

  def getTimeAndSize(line: String) = {
    var res = ("", 0L)

    try {
      val httpSizePattern(code, size) = line
      val timePattern(year, hour) = line
      res = (hour, size.toLong)
    } catch {
      case e : Exception => e.printStackTrace()
    }
    res
  }
}
