package geektime.ml

import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *数据二值化处理
  */
object BinarizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("BinarizerExample")
      .config("truncate","false")
      .getOrCreate()
    import  spark.implicits._
      //构造数据集:tuple中第一值为特征标签、第二个值为特征值
    val df = Seq((0,0.3),(1,0.4),(1,0.8),(0,0.45)).toDF("label", "feature")
  //定义二值化实例Binarizer，其中InputCol为输入的特征，既就是要进行二值化的数据；OutputCol而二值化后结果的输出列，
  // Threshold表示特征的阈值，当特征大于该值是返回1.0，当特征小于该值是返回0.0，
    val binarizer = new Binarizer().setInputCol("feature").setOutputCol("binarizer_feature").setThreshold(0.5)
  //对df进行二值化处理
    val binarizerDF = binarizer.transform(df)
    //将二值化处理后的结果查询输出
    val binarizerFeature = binarizerDF.select("binarizer_feature")
    binarizerFeature.collect().foreach(x=>println(x))
  }
}
