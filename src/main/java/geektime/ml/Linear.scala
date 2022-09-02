package geektime.ml

import org.apache.spark.{SparkConf, SparkContext}

object Linear {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils

    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local")
    val sc = new SparkContext(conf)


    val basePath = "./src/main/resources/"

    //1:数据加载
    // 将LIBSVM格式的带有二元特征标签的数据加载到RDD中，并自动进行特征识别和分区数量设置
    val data = MLUtils.loadLibSVMFile(sc, basePath+"mllib/sample_libsvm_data.txt")

    // 将数据按照60%（0.6）和40%（0，4）的比例进行拆分，60%部分的数据用于作为训练数据集， 40%部分的数据用于作为测试数据集.
    //seed为数据抽样过程中使用的随机数
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    //取出训练数据集
    val training = splits(0).cache()
    //取出测试数据集
    val test = splits(1)
   //2：模型训练
    // 运行基于线性回归的SVM训练算法，建立模型
    val numIterations = 100//要运行的梯度下降的迭代次数
    val model = SVMWithSGD.train(training, numIterations)

    // 清除阈值，以便“预测”将输出原始预测分数
    model.clearThreshold()
   //3：模型预测
    // 在测试数据集上利用训练好的模型对测试数据进行预测，获取计算后的得分
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // 4：获取评价指标，多模型进行评估
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auc = metrics.areaUnderROC()

    /**
      * 从AUC 判断分类器（预测模型）优劣的标准：
      * AUC = 1，是完美分类器。
      * AUC = [0.85, 0.95], 效果很好
      * AUC = [0.7, 0.85], 效果一般
      * AUC = [0.5, 0.7],效果较低，但用于预测股票已经很不错了
      * AUC = 0.5，跟随机猜测一样（例：丢铜板），模型没有预测价值。
      * AUC < 0.5，比随机猜测还差；但只要总是反预测而行，就优于随机猜测。
      */
    println(s"Area under ROC = $auc")

    // 模型保存和加载
//    model.save(sc, basePath+"/scalaSVMWithSGDModel")
//    val sameModel = SVMModel.load(sc, basePath+"/scalaSVMWithSGDModel")

  }
}
