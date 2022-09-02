package geektime.streaming

import java.io.FileWriter

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.execution.streaming.sources.ForeachBatchSink

/**
  * example data :
  cat dog
  dog dog

   owl cat

   dog
   owl
  */
object StructStreaming {
  def main(args: Array[String]): Unit = {
    //1:定义 SparkSession；
    val spark = SparkSession
      .builder
      .appName("StructStreaming")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    //2:定义 DataFrame，注意这里使用 spark.readStream()监听 TCP 端口的数据并实时转换为 DataFrame；
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    //3:定义 DataFrame 操作，这里基于 DataFrame 做聚合操作；
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    wordCounts.printSchema()
    val query = wordCounts.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("complete")
      .format("console")
  /*  .foreach(new ForeachWriter[Row] {
     //对每行数据进行处理
      override def process(value: Row): Unit = {
        println(value.mkString(","))
      }
      //close的时候关闭资源，提交事务
      override def close(errorOrNull: Throwable): Unit = {
      }
      //open的时候初始化资源，开启事务
      override def open(partitionId: Long, version: Long): Boolean = {
        true
      }
    })*/
     // .foreachBatch( (batchDF: DataFrame, batchId: Long) => println(batchDF.collectAsList(),batchId))
      .start()//4:启动 StreamingQuery 实时流计算。

    query.awaitTermination()

  }
}
