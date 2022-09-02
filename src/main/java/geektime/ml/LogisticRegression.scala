package geektime.ml

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils

    val conf = new SparkConf().setAppName("LogisticRegression").setMaster("local")
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

    //    //2：模型训练
    //    // 运行训练算法，建立模型
    /**
      * MLlib支持两种优化算法求解逻辑回归问题，小批量梯度下降（Mini-batch Gradient Descent）和改进的拟牛顿法（L-BFGS），分别对应Logistic Regression With SGD和Logistic Regression With LBFGS，实际工作中，对于特征规模很大的逻辑回归模型，使用改进的拟牛顿法L-BFGS加快求解速度。
      */


    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10) //设置可能分类的结果数，这里表示可能有10中分类结果
      .run(training)

    //3：模型预测
    // 在测试数据集上利用训练好的模型对测试数据进行预测，获取计算后的得分
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // 4：获取评价指标，多模型进行评估
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val auc = metrics.accuracy
    println(s"Accuracy = $auc")

    // 模型保存和加载
    model.save(sc, basePath+"scalaLogisticRegressionWithLBFGSModel")
    val sameModel = LogisticRegressionModel.load(sc,
      basePath+"scalaLogisticRegressionWithLBFGSModel")

  }
}
