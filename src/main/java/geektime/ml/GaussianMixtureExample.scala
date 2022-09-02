package geektime.ml

import java.util

import com.github.sh0nk.matplotlib4j.Plot
import com.github.sh0nk.matplotlib4j.builder.HistBuilder
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import spire.math.Number

object GaussianMixtureExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GaussianMixtureExample").setMaster("local")
    val sc = new SparkContext(conf)

    val basePath = "./src/main/resources/"
    //1： 加载并解析数据
    val data = sc.textFile(basePath+"mllib/gmm_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble))).cache()
    println("============将数据绘制在散点图上观察数据分布================")

    parsedData.foreach(x=>print(x.apply(1)+","))


    // 2：模型训练，通过GaussianMixture将数据聚类为2类
    val gmm = new GaussianMixture().setK(2).run(parsedData)

     //3:模型评估：模型参数输出 weights权重，mu 表示均值均值对应正态分布的中间位置，sigma表示标准差，标准差衡量了数据围绕均值分散的程度。
        for (i <- 0 until gmm.k) {
          println("weight=%f\nmu=%s\nsigma=\n%s\n" format
            (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
        }

    /**
      *
      * weight=0.481001
      * mu=[0.07217410383234305,0.01667014635731041]
      * sigma=
      * 4.776372230456057   1.8744808448933612
      * 1.8744808448933612  0.9140539747885537
      *
      * weight=0.518999
      * mu=[-0.10457961871435074,0.04289537152746085]
      * sigma=
      * 4.910299881346857   -2.008504364386921
      * -2.008504364386921  1.0120950661848187
      */
    //4：模型使用：利用GaussianMixture模型对数据聚类分析
    gmm.predict(parsedData).foreach(x=>println(x))
    // 5：保存和加载模型
//    gmm.save(sc, basePath+"GaussianMixtureExample/GaussianMixtureModel")
//    val sameModel = GaussianMixtureModel.load(sc,
//      basePath+"GaussianMixtureExample/GaussianMixtureModel")





  }
}
