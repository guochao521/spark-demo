package geektime.ml

import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.tree.DecisionTree
    import org.apache.spark.mllib.tree.model.DecisionTreeModel
    import org.apache.spark.mllib.util.MLUtils


    val conf = new SparkConf().setAppName("SummaryStatisticsExample").setMaster("local")
    val sc = new SparkContext(conf)


    val basePath = "./src/main/resources/"
    //1： 加载并解析数据

    val data = MLUtils.loadLibSVMFile(sc, basePath+"mllib/sample_libsvm_data.txt")
    // 对数据按照70%和20%进行进行随机分类，70%的数据为训练数据集，30%的数据为验证数据集
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    //2： 模型训练 ，训练一个决策树模型（ DecisionTree model）
    val numClasses = 2  //分类个数
    val categoricalFeaturesInfo = Map[Int, Int]()//使用存储分类特征的集合，空的分类特征信息表示所有特征都是连续的
    val impurity = "gini" //不纯度：用于信息增益计算的标准，支持 "gini（基尼系数）" (recommended) or "entropy（信息熵）".
    val maxDepth = 5 //树最大深度
    val maxBins = 32//maxBins：对连续特征离散化时采用的桶数。

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    //3:模型评估：利用测试数据集对模型进行评估
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    /**
      * Test Error = 0.0
      Learned classification tree model:
         DecisionTreeModel classifier of depth 2 with 5 nodes
          If (feature 434 <= 70.5)
           If (feature 100 <= 193.5)
            Predict: 0.0
           Else (feature 100 > 193.5)
            Predict: 1.0
          Else (feature 434 > 70.5)
           Predict: 1.0
      */

    // Save and load model
    model.save(sc, basePath+"myDecisionTreeClassificationModel")
    val sameModel = DecisionTreeModel.load(sc, basePath+"myDecisionTreeClassificationModel")
  }
}
