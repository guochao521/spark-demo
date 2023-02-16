package customize.operator

import org.apache.spark.{SparkConf, SparkContext}

// 算子总结：https://developer.aliyun.com/article/653927
object cogroup {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("cogroup demo")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    val rdd1 = sc.parallelize(Array(("aa", 1), ("bb", 2), ("cc", 6)))
    val rdd2 = sc.parallelize(Array(("aa", 3), ("dd", 4), ("aa", 5)))

    val rdd3 = rdd1.cogroup(rdd2).collect()

    for (i<- rdd3.indices) {
      println(rdd3(i))
    }
  }
}
