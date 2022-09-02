package geektime.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// exampleon
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.util.MLUtils
// 特征正则化

object NormalizerExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("NormalizerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val basePath = "./src/main/resources/"
    // 将LIBSVM格式的带有二元特征标签的数据加载到RDD中，并自动进行特征识别和分区数量设置
    val data = MLUtils.loadLibSVMFile(sc, basePath+"mllib/sample_libsvm_data.txt")
    val normalizer1 = new Normalizer()
    // 对data1 中的每个数据使用二阶范式进行正则化
    val data1 = data.map(x => (x.label, normalizer1.transform(x.features)))
    println("data1: ")
    data1.collect.foreach(x => println(x))
    sc.stop()
  }
}
