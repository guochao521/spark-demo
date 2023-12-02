package customize.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/12/1 22:30
 * @description 基站分析：基站停留时间TOPN
 */

object BaseStationAnalyze {
  """
    |基站停留时间TOPN：
    | 根据用户产生的日志信息，分析在哪个基站停留的时间最长
    | 在一定范围内，求所有用户经过的所有基站所停留时间最长的TOP2
    |""".stripMargin

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseStationAnalyze").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 1.获取用户访问基站的日志
    val fileRDD = sc.textFile("./src/main/resources/spark_core/19735E1C66.log")
    // 2.将日志转换为元组
    val userInfoRDD = fileRDD.map(
      line => {
        val fields = line.split(",")
        val phone = fields(0)
        val time = fields(1).toLong
        val lac = fields(2)
        val eventType = fields(3)
        val timeLong = if (eventType.equals("1")) -time else time
        ((phone, lac), timeLong)
      }
    )
    // 3.聚合
    val sumRDD = userInfoRDD.reduceByKey(_ + _)
    // 4.转换数据格式，进行 join 操作
    val lacAndPt = sumRDD.map(
      tup => {
        val phone = tup._1._1
        val lac = tup._1._2
        val time = tup._2
        (lac, (phone, time))
      }
    )
    // 5.加载基站信息
    val lacInfo = sc.textFile("./src/main/resources/spark_core/lac_info.txt")
    // 6.切分基站信息
    val lacAndXY = lacInfo.map(line => {
      val fields = line.split(",")
      val lac = fields(0)
      val x = fields(1)
      val y = fields(2)
      (lac, (x, y))
    })
    // 7.连接操作
    val joinedRDD = lacAndPt.join(lacAndXY)
    // 8.对数据进行整合
    val phoneAndTXY = joinedRDD.map(tup => {
      val phone = tup._2._1._1
      val time = tup._2._1._2
      val xy = tup._2._2
      (phone, (time, xy))
    })
    // 9.按照手机进行分组
    val groupedRDD = phoneAndTXY.groupByKey()
    // 10.排序
    val sortedRDD = groupedRDD.mapValues(_.toList.sortBy(_._2).reverse)
    // 11.整合
    val resultRDD = sortedRDD.map(tup => {
      val phone = tup._1
      val list = tup._2
      val filterList = list.map(l => {
        val time = l._1
        val xy = l._2
        (time, xy)
      })
      (phone, filterList)
    })
    val resultList = resultRDD.mapValues(_.take(2))
    println(resultList.collect().toList)
    // List((18101056888,List((54000,(116.304864,40.050645)), (1900,(116.303955,40.041935)))), (18688888888,List((51200,(116.304864,40.050645)), (1300,(116.303955,40.041935)))))
  }
}
