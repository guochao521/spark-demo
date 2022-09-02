package geektime.ml



import java.util

import com.github.sh0nk.matplotlib4j.Plot
import com.github.sh0nk.matplotlib4j.builder.HistBuilder
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import spire.math.Number
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors


object KMeansExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local")
    val sc = new SparkContext(conf)


    val basePath = "./src/main/resources/"
    //1： 加载并解析数据
    val data = sc.textFile(basePath+"mllib/kmeans_data.txt")

    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    parsedData.foreach(x=>println(x))
    //2：模型训练： 使用KMeans算法将数据聚类为2类，返回KMeansModel
    val k = 2 //聚类个数为2
    val maxIterations = 20 //最大迭代次数为20
    val kMeansModel = KMeans.train(parsedData, k, maxIterations)

    //3：模型评估：通过计算点到其最近中心的距离平方的总和来评估聚类


    val WSSSE = kMeansModel.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
   //4：利用KMeans模型对数据聚类
    kMeansModel.predict(parsedData).foreach(x=>println(x))

    // 5：保存并加载模型
//    kMeansModel.save(sc, basePath+"KMeansExample/KMeansModel")
//    val sameModel = KMeansModel.load(sc, basePath+"KMeansExample/KMeansModel")

    sc.stop()



  }
}
