package geektime.ml

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

object ASL {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ASL").setMaster("local")
    val sc = new SparkContext(conf)

    val basePath = "./src/main/resources/"

    //1:数据加载
    // 将LIBSVM格式的带有二元特征标签的数据加载到RDD中，并自动进行特征识别和分区数量设置

    // Load and parse the data
    val data = sc.textFile(basePath+"mllib/als/test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // 2：基于 ALS训练推荐模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
   //3:基于模型对用户感兴趣产品进行预测推荐
    // 获取部分用户产品数据
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    //对用户感兴趣的产品进行预测
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
   //4:模型评估
    //将预测结果和训练数据进行JOIN，以便对比预测准确性
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)
    //计算真实数据和预测结果的均方差
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println(s"Mean Squared Error = $MSE")

    //  模型保存和加载
//    model.save(sc, basePath+"myCollaborativeFilter")
//    val sameModel = MatrixFactorizationModel.load(sc, basePath+"myCollaborativeFilter")


  }
}
