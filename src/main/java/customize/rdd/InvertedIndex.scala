package customize.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat, FileSplit}
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      throw new Exception("Parameter error, Must give input path and output path")
    }

    val inputPath: String = args.apply(0)
    val outputPath: String = args.apply(1)

    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")

    val sc = new SparkContext(sparkConf)

    val fc = classOf[TextInputFormat]
    val kc = classOf[LongWritable]
    val vc = classOf[Text]
    val text = sc.newAPIHadoopFile(inputPath, fc, kc, vc, sc.hadoopConfiguration)

    val linesWithFileNames = text.asInstanceOf[NewHadoopRDD[LongWritable, Text]].mapPartitionsWithInputSplit((inputSplit, iterator) => {
      val file = inputSplit.asInstanceOf[FileSplit]
      iterator.map(tup => (file.getPath.toString.split("/").last, tup._2))
    })

    val tempIndex = linesWithFileNames.flatMap {
      case (fileName, text) => text.toString.split("\r\n")
        .flatMap(line => line.split(" "))
        .map{ word => (word, fileName)}
    }

    val invertedIndex = tempIndex.groupByKey()

    val group = invertedIndex.map {
      case(word, tup) =>
        val fileCountMap = scala.collection.mutable.HashMap[String, Int]()
        for (fileName <- tup){
          val count = fileCountMap.getOrElseUpdate(fileName, 0) + 1
          fileCountMap.put(fileName, count)
        }
        (word, fileCountMap)
    }.sortByKey().map(word => s"${word._1}:${word._2}")

    group.repartition(numPartitions = 1).saveAsTextFile(outputPath)

  }
}
