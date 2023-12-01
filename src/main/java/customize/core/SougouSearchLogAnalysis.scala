package customize.core

import com.hankcs.hanlp.HanLP
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/12/1 17:24
 * @description 对SougouSearchLog进行分词并统计如下指标:
 *  1.热门搜索词
 *  2.用户热门搜索词(带上用户id)
 *  3.各个时间段搜索热度
 * 链接：https://lilinchao.com/archives/1604.html
 */
object SougouSearchLogAnalysis {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SougouSearchLogAnalysis").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // 1.加载数据
    val lines = sc.textFile("./src/main/resources/spark_core/SogouQ.sample")
    // 2.数据处理
    val SogouRecordRDD = lines.map(
      line => {
        val arr = line.split("\\s+")
        SogouRecord(
          arr(0),
          arr(1),
          arr(2),
          arr(3).toInt,
          arr(4).toInt,
          arr(5)
        )
      }
    )
    // 2.1 切割数据
    val wordsRDD = SogouRecordRDD.flatMap(
      record => {
        // 取出搜索词
        val wordsStr = record.queryWords.replaceAll("\\[|\\]", "")
        // 将 Java 集合转换为 Scala 集合
        import scala.collection.JavaConverters._
        // 对搜索词进行分词
        HanLP.segment(wordsStr).asScala.map(_.word)
      }
    )

    // 3.统计指标
    // 3.1 热门搜索词
    val result1 = wordsRDD
      .filter(word => !word.equals(".") && !word.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    // 3.2 用户热门搜索词（带上用户 id）
    val userIdAndWordRDD = SogouRecordRDD.flatMap(
      record => {
        val wordsStr = record.queryWords.replaceAll("\\[|\\]", "")
        import scala.collection.JavaConverters._ //将Java集合转为scala集合
        val words = HanLP.segment(wordsStr).asScala.map(_.word)
        val userId = record.userId
        words.map(word => (userId, word))
      }
    )
    val result2 = userIdAndWordRDD
      .filter(t=> !t._2.equals(".") && !t._2.equals("+"))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
    // 3.3 各个时间段搜索热度
    val result3 = SogouRecordRDD.map(
      record =>  {
        val timeStr = record.queryTime
        val hourAndMinuteStr = timeStr.substring(0, 5)
        (hourAndMinuteStr, 1)
      }
    ).reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)

    // 4.打印结果
    result1.foreach(println)
    result2.foreach(println)
    result3.foreach(println)

    sc.stop()
  }

  /**
   * 用户搜索点击网页记录Record
   *
   * @param queryTime  访问时间，格式为：HH:mm:ss
   * @param userId     用户ID
   * @param queryWords 查询词
   * @param resultRank 该URL在返回结果中的排名
   * @param clickRank  用户点击的顺序号
   * @param clickUrl   用户点击的URL
   */
  case class SogouRecord(
    queryTime: String,
    userId: String,
    queryWords: String,
    resultRank: Int,
    clickRank: Int,
    clickUrl: String
  )
}


