package customize.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object InvertedIndexRDD {

  def main(args: Array[String]): Unit = {

    val input = "C:\\Users\\wangguochao\\Desktop\\testdata\\input"
    val output = "C:\\Users\\wangguochao\\Desktop\\testdata\\output"

    //    if(args.length != 2){
    //      throw new Exception("Parameter error, Must give input path and output path")
    //    }

    // 还可以使用 SparkSession
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("INFO")

    // 加载指定目录下文件
    //    val textFile = sc.wholeTextFiles(args.apply(0))
    val textFile = sc.wholeTextFiles(input)
    // 读取进来的是文件路径
    val fileNameLength = textFile.map(x => x._1.split("/").length).collect()(0)
    val filesContext = textFile.map(x => (x._1.split("/")(fileNameLength - 1), x._2)).sortByKey()

    // 创建单词列表
    val wordsList = filesContext.flatMap(x => {
      // 根据行切分句子
      val lines = x._2.split("\n")
      // 生成的（文件名，单词）对用链表组织
      // list 的第一个节点为 null
      val list = mutable.LinkedList[(String, String)]()

      // 将假头指向临时节点
      var temp = list

      // 每一行切分单词，然后每个单词组成（文件名，单词）
      for (index <- 0 until lines.length) {
        val words = lines(index).split(" ").iterator
        while (words.hasNext) {
          temp.next = mutable.LinkedList[(String, String)]((words.next(), x._1))
          temp = temp.next
        }
      }

      // 删除第一个 null 节点
      val wordListEnd = list.drop(1)
      wordListEnd
    })

    // 拼接单词
    val resultRDD = wordsList.groupByKey().map({
      case (word, tup) => {
        val fileCountMap = mutable.HashMap[String, Int]()
        for (fileName <- tup) {
          val count = fileCountMap.getOrElseUpdate(fileName, 0) + 1
          fileCountMap.put(fileName, count)
        }
        (word, fileCountMap)
      }
    }).sortByKey()

    resultRDD.collect().foreach(println)
    resultRDD.saveAsTextFile(output)
    sc.stop()
  }
}
