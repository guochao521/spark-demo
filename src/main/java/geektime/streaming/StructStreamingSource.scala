package geektime.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * example data :
  cat dog
  dog dog

   owl cat

   dog
   owl
  */
object StructStreamingSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructStreamingSource")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

   /* val dataSource = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()*/

  /*  val dataSource = spark.readStream
      .format("rate")
      //每秒1条
      .option("rowsPerSecond", 1)
      .option("numPartitions", 1)
      .load()*/

    val basePath = "./src/main/resources/"


    val schema = StructType {
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }

    val dataSource = spark.readStream
       .schema(schema)//schema信息
      .format("json")//数据格式
      .option("latestFirst","true")//spark Options信息，是否首先处理最新的新文件，当存在大量积压文件时很有用
      .option("fileNameOnly",true)//spark Options信息，是否仅基于文件名而不是基于完整路径检查新文件（默认值：false）。设置为true时，以下文件将被视为同一文件，因为它们的文件名dataset.txt是相同的
      .load(basePath + "structured_dir")//数据源
    //"file:///dataset.txt"
    //"s3://a/dataset.txt"


      dataSource.printSchema()
    val query = dataSource.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
