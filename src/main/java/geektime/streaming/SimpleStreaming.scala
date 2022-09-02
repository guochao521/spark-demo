package geektime.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SimpleStreaming {

  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.Durations
  import org.apache.spark.streaming.api.java.JavaDStream
  import org.apache.spark.streaming.api.java.JavaPairDStream
  import org.apache.spark.streaming.api.java.JavaReceiverInputDStream
  import org.apache.spark.streaming.api.java.JavaStreamingContext
  import scala.Tuple2
  import java.util

  /**
    * nc -lk 9999
    *
    * @param args
    */
    def main(args: Array[String]): Unit = {
      try {

        val checkpointDir ="./src/main/resources/checkpoint"

       //1：创建一个本地SparkConf
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        //2：创建一个StreamingContext，每1s处理一次数据
        val ssc = new StreamingContext(conf, Seconds(10))
        ssc.checkpoint(checkpointDir)
        //3：创建一个DStream并监听Localhost的9999端口
         val lines = ssc.socketTextStream("localhost", 9999)
        //4:调用flatMap、map、reduceByKey、操作
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map(word => (word, 1))
        //val wordCounts = pairs.reduceByKey(_ + _)
        // 5：打印在DStream中每个RDD的前10行数据
        //        wordCounts.print()
        //        System.out.println("wordCounts:" + wordCounts)
        //        wordCounts.print()
        words.foreachRDD { rdd =>
          // Get the singleton instance of SparkSession
          val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
          import spark.implicits._
          // Convert RDD[String] to DataFrame
          val wordsDataFrame = rdd.toDF("word")
          // Create a temporary view
          wordsDataFrame.createOrReplaceTempView("words")

          // Do word count on DataFrame using SQL and print it
          val wordCountsDataFrame =
            spark.sql("select word, count(*) as total from words group by word")
          wordCountsDataFrame.show()
        }



        //6：调用start方法启动流计算
        ssc.start()
        //7：等待流计算完成
        ssc.awaitTermination()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }


}
