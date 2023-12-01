package customize.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/12/1 16:27
 *
 * 统计出每一个省份每个广告被点击数量排行的Top3
 * https://lilinchao.com/archives/1602.html
 */
object SparkCoreAgentDemo {

  def main(args: Array[String]): Unit = {

    // 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val dataRDD = sc.textFile("./src/main/resources/spark_core/agent.log")
    // 2. 将原始数据进行结构转换，方便统计：时间戳，省份，城市，用户，广告
    // ((省份，广告)，1)
    val mapRDD = dataRDD.map(
      line => {
        val dataArray = line.split(" ")
        ((dataArray(1), dataArray(4)), 1)
      }
    )
    // 3.将转换结构后的数据进行分组聚合
    //  ((省份，广告)，1) => ((省份，广告)，sum)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    // 4.将聚合后的数据进行结构转换
    // ((省份，广告)，sum) => (省份，(广告，sum))
    val newRDD = reduceRDD.map {
      case ((province, ad), sum) => (province, (ad, sum))
    }
    // 5.将转换结构后的数据根据省份进行分组
    val groupRDD = newRDD.groupByKey()
    // 6.将分组后的数据组内排序（降序），取前3名
    val resultRDD = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    // 7.将结果采集到控制台打印出来
    resultRDD.foreach(println)
    // 关闭环境
    sc.stop()
  }

}
