package geektime.ml


import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// exampleon
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
// 常用的数据统计方法

object SummaryStatisticsExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local")
    val sc = new SparkContext(conf)

    // exampleon
    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )
    //****************************** 计算列上的统计信息******************************
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean)  // 计算均值
    println(summary.variance)  // 计算方差
    println(summary.numNonzeros)  //统计非0值
    println(summary.max)
    println(summary.min)
    println(summary.normL1)//1阶范数
    println(summary.normL2)//1阶范数

    //***********************计算相关性***************************************

    val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5))  // a series
    // must have the same number of partitions and cardinality as seriesX
    val seriesY: RDD[Double] = sc.parallelize(Array(110, 22, 33, 33, 555))
    val seriesY_1: RDD[Double] = sc.parallelize(Array(210, 200, 150, 100, 50))

    //计算pearson（皮尔逊）和spearman（斯皮尔曼）的相关性，默认为pearson相关性
    //计算相关性的两个RDD必须有相同的分区（partions）和数据个数
    val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(s"seriesX and seriesY pearson correlation is: $correlation")
    //seriesX and seriesY pearson correlation is: 0.750393986770354
    //seriesX 和 seriesY 都是增长趋势的数据集，因此其相关系数为 0.75 左右，是正相关。

    val correlation_1: Double = Statistics.corr(seriesX, seriesY_1, "pearson")
    println(s"seriesX and seriesY_1 pearson correlation is: $correlation_1")
    //seriesX and seriesY_1 pearson correlation is: -0.9424587872925675
    // seriesX增长，seriesY_1 为下降趋势的数据集，因此 seriesX 和 seriesY_1 的相关系数为 -0.94 左右，既就是其相关性为负相关。

    val correlation_spearman: Double = Statistics.corr(seriesX, seriesY, "spearman")
    println(s"seriesX and seriesY spearman correlation is: $correlation_spearman")
    //seriesX and seriesY spearman correlation is: 0.36842105263157904
  //seriesX 和 seriesY 都是增长趋势的数据集，因此其相关系数为 0.368 左右，是正相关。
    val correlation_spearman_1: Double = Statistics.corr(seriesX, seriesY_1, "spearman")
    println(s"seriesX and seriesY_1 spearman correlation is: $correlation_spearman_1")
    //seriesX and seriesY_1 spearman correlation is: -0.9746794344808964
    // seriesX增长，seriesY_1 为下降趋势的数据集，因此 seriesX 和 seriesY_1 的相关系数为 -0.974679 左右


    // val correlation: Double = Statistics.corr(seriesX, seriesY, "spearman")


    val data: RDD[Vector] = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(5.0, 33.0, 366.0))
    )  // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
    // If a method is not specified, Pearson's method will be used by default.
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(correlMatrix.toString)



    val dataSeq = sc.parallelize(Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')))
    // specify the exact fraction desired from each key
    val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)
    // 根据不同的key的抽样因子获取近似抽样
    val approxSample = dataSeq.sampleByKey(withReplacement = false, fractions = fractions)
    println("##############approxSample##################")
    approxSample.foreach(x=>println(x))
    // 根据不同的key的抽样因子获取精确抽样
    val exactSample = dataSeq.sampleByKeyExact(withReplacement = false, fractions = fractions)
    println("##############exactSample##################")
    exactSample.foreach(x=>println(x))

    sc.stop()
  }
}

