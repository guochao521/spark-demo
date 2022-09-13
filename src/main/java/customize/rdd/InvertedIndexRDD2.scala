package customize.rdd

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndexRDD2 {

  def main(args: Array[String]): Unit = {

    val input = "./input/sparkData"
    val output = "./output/InvertedIndex"

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    // 1. 获取hadoop操作文件的api
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // 2. 读取目录下的文件，生成文件列表
    val fileList = fs.listFiles(new Path(input), true)

    // 3. 遍历文件，并读取文件内容，生成rdd，结构为（文件名，单词）
    var unionRDD = sc.emptyRDD[(String, String)]

    while (fileList.hasNext) {
      val absPath = new Path(fileList.next().getPath.toString)
      val fileName = absPath.getName
      val rdd = sc.textFile(absPath.toString).flatMap(_.split(" ").map((fileName, _)))

      // 4. 将遍历的多个RDD拼接成一个RDD
      unionRDD = unionRDD.union(rdd)
    }

    // 5. 构建词频（（文件名，单词），词频）
    val wordRDD = unionRDD.map(word => {
      (word, 1)
    }).reduceByKey(_ + _)
    println("*************词频RDD*******************")
    wordRDD.foreach(println)

    // 6. 调整数据格式，将（（文件名，单词），词频）=> （单词，（文件名，词频）） => （单词，（文件名，词频）） 汇总输出
    val formatRDD1 = wordRDD.map(word => {
      (word._1._2, String.format("(%s,%s)", word._1._1, word._2.toString))
    })

    println("************调整数据格式 阶段1***********")
    formatRDD1.foreach(println)

    val formatRDD2 = formatRDD1.reduceByKey(_ + "," + _)
    println("************调整数据格式 阶段2***********")
    formatRDD2.foreach(println)
    val formatRDD3 = formatRDD2.map(word => String.format("\"%s\", {%s}", word._1, word._2))
    println("************调整数据格式 阶段3 (最后输出结果)（***********")
    formatRDD3.foreach(println)

    formatRDD3.repartition(1).saveAsTextFile(output)
    sc.stop()
  }
}
