package geektime.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
//特征归一化

object StandardScalerExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StandardScalerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val basePath = "./src/main/resources/"
    /*
      将LIBSVM格式的带有二元特征标签的数据加载到RDD中，并自动进行特征识别和分区数量设置
     */
    val data = MLUtils.loadLibSVMFile(sc, basePath+"mllib/sample_libsvm_data.txt")
    //计算均值和方差并将其存储为StandardScalerModel模型【"mean"：均值, "std"：标准差】
    val scaler1 = new StandardScaler().fit(data.map(x => x.features))
    // 对data中的每个数据，在向量上应用标准差标准化，最终得到归一化结果
    val data1 = data.map(x => (x.label, scaler1.transform(x.features)))
    println("data1: ")
    data1.collect.foreach(x => println(x))
    sc.stop()
  }
}

