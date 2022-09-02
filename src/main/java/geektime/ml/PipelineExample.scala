package geektime.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vector
object PipelineExample {
  def main(args: Array[String]): Unit = {

    val basePath = "./src/main/resources/"
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PipelineExample")
      .getOrCreate()

    // 1：准备训练数据：个格式为包含  (id, text, label) tuples的list
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    // 2：配置一个 ML pipeline,包括三个阶段: tokenizer（分词）, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    //HashingTF 是一个Transformer，在文本处理中，接收词条的集合然后把这些集合转化成固定长度的特征向量。这个算法在哈希的同时会统计各个词条的词频。

    val hashingTF = new HashingTF()
      .setNumFeatures(1000)//特征数为1000,	支持最大的特征数量	默认值：262144
      .setInputCol(tokenizer.getOutputCol)//DF中待变换的特征，特征类型必须为：Array
      .setOutputCol("features")//变换后的特征名称，转换后的类型为：vector
    val lr = new LogisticRegression()
      .setMaxIter(10) //最大迭代次数为10
      .setRegParam(0.001)//正则化（regularization）参数为0.001
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    //3： 使用管道（pipeline）训练模型
    val model = pipeline.fit(training)

    // 4：模型保存
    model.write.overwrite().save(basePath+"/spark-logistic-regression-model")


    //5：加载模型，构造测试数据集并预测
    val sameModel = PipelineModel.load(basePath+"/spark-logistic-regression-model")

    // 定义测试数据, 这里为未标签haul的(id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    //在模型上调用transform对测试数据集合（test）进行预测
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    /**
      (4, spark i j k) --> prob=[0.15964077387874098,0.840359226121259], prediction=1.0
      (5, l m n) --> prob=[0.8378325685476612,0.16216743145233883], prediction=0.0
      (6, spark hadoop spark) --> prob=[0.06926633132976263,0.9307336686702373], prediction=1.0
      (7, apache hadoop) --> prob=[0.9821575333444208,0.017842466655579155], prediction=0.0
      */

  }
}
