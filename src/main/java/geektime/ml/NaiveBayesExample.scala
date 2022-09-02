package geektime.ml

import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
    import org.apache.spark.mllib.util.MLUtils


    val conf = new SparkConf().setAppName("StandardScalerExample").setMaster("local")
    val sc = new SparkContext(conf)
    val basePath = "./src/main/resources/"
     //1：数据加载
    /*
      将LIBSVM格式的带有二元特征标签的数据加载到RDD中，并自动进行特征识别和分区数量设置
     */
    val data = MLUtils.loadLibSVMFile(sc, basePath+"mllib/sample_libsvm_data.txt")

    // 对数据按照60%和40%进行进行随机分类，60%的数据为训练数据集，40%的数据为验证数据集
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))
   //2:模型训练：利用NaiveBayes对模型进行训练
    /**
      *  lambda 为平滑因子
      *  modelType为模型类型，可以为multinomial（二项式）和bernoulli（贝努利）
      */
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
  //3:模型评估：利用模型对测试数据集进行预测，其中model.predict(p.features)为预测的分类， p.label为实际的分类
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    //计算模型的准确率，（x => x._1 == x._2).count()表示预测正确的结构数， test.count()表示数据总数
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy:"+accuracy)

    /**
      * accuracy:0.9696969696969697
      */
    //模型保存和加载
    model.save(sc, basePath+"/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, basePath+"myNaiveBayesModel")
  }
}
