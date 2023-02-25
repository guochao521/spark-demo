package customize.core

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/2/24 21:32
 * @deprecated 影评分析（年份，电影id，电影名字，平均评分）
 */

/**
 * 链接：https://www.lilinchao.com/archives/1614.html
 *
 * 其他例子：
 *  利用Spark求区域用户访问量（每个省的访问量）:
 *    https://blog.csdn.net/weixin_40903057/article/details/88421952
 *  统计每个域名下，不同的URL对应的访问次数的top3：
 *    https://blog.csdn.net/weixin_40903057/article/details/88421952
 */

/**
 * 需求：影评分析
 * - 按照年份进行分组。计算每部电影的平均评分，平均评分保留小数点后一位，并按评分大小进行排序。
 * - 评分一样，按照电影名排序。相同年份的输出到一个文件中。
 *   结果展示形式（年份，电影id，电影名字，平均评分）
 * 要求：尝试使用自定义分区、自定义排序和缓冲。
 */
object MovieReviewAnalyzer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MovieReviewAnalyzer").setMaster("local[1]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val basePath = "./src/main/resources/spark_core/"

    /**
     * ratings.dat用户评分文件:
     *  第一列：用户ID
        第二列：电影ID
        第三列：评分
        第四列：评分时间戳
     */
    // 1.先读电影ID和评分
    val rdd2 = sc.textFile(basePath + "ratings.dat")
    // 2.按指定分隔符切分并过滤脏数据
    val filterData = rdd2.map(_.split("::")).filter(_.length >= 4)
    // 3.去掉不必要的字段，并按电影ID分组，计算每部电影的平均分
    val movieGroupRDD = filterData.map(arr => {
      (arr(1), arr(2).toDouble) // (1193,5.0)
    }).groupBy(_._1) // (2828,CompactBuffer((2828,1.0), (2828,2.0), (2828,5.0), ..., (2828,2.0)))

//    movieGroupRDD.take(1).foreach(x => println(x.toString()))

    val movieAvg = movieGroupRDD.mapValues(iter => {
      val num: Int = iter.size
      val sum = iter.map(_._2).sum
      (sum / num).formatted("%.1f")
    })
//    movieAvg.take(10).foreach(x => println(x.toString()))

    // 4.截取电影id、电影名字、年份
    val movies = sc.textFile(basePath + "movies.dat")
    val moviesFilterRDD = {
      movies.map(_.split("""::""")).filter(_.length >= 3)
    }
    val moviesMapRDD = moviesFilterRDD.map(arr => {
      val movieId = arr(0)
      val movieName = arr(1)
      val year = arr(1).substring(arr(1).length - 5, arr(1).length - 1)
      (movieId, (year, movieName))
    })

    // 5.将平均评分和电影数据进行join，将数据封装到对象中
    val allData = moviesMapRDD.join(movieAvg) // (2828,((1999,Dudley Do-Right (1999)),2.1))
      .map(x => {
        val year = x._2._1._1
        val movieId = x._1
        val movieName = x._2._1._2
        val avg = x._2._2.toFloat
        (year, MovieBean(year, movieId, movieName, avg)) // (1999,MovieBean(1999,2828,Dudley Do-Right (1999),2.1))
      })

    val years: Array[String] = allData.map(_._1).distinct().collect()

    val resultRDD = allData.partitionBy(new MyPartitioner(years))
      .mapPartitions(iter => {
        iter.toList.sortBy(_._2).toIterator
      }).map(t => t._2.year + "," + t._2.movieId + "," + t._2.movieName + "," + t._2.avg)

    resultRDD.foreach(x => println(x))
//    resultRDD.saveAsTextFile("./src/main/resources/spark_core/tmp/movie")

    sc.stop()
  }
}

// 自定义分区器，通过年份进行分区
class MyPartitioner(years: Array[String]) extends Partitioner {
  override def numPartitions: Int = years.length

  override def getPartition(key: Any): Int = {
    val year = key.asInstanceOf[String]
    years.indexOf(year)
  }
}

// 自定义对象封装数据，并实现对应的比较逻辑
case class MovieBean(year: String, movieId: String, movieName: String, avg: Float) extends Ordered[MovieBean] {
  override def compare(that: MovieBean): Int = {
    // 定义排序顺序
    if (that.avg == this.avg) {
      if (that.movieName.compareTo(this.movieName) > 0)
        -1
      else
        1
    } else {
      if (that.avg > this.avg)
        1
      else
        -1
    }
  }
}

