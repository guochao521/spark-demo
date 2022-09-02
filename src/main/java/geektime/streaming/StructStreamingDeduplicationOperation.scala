package geektime.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
  * start zookeeper :cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-zookeeper-3.5.5-bin && bin/zkServer.sh start
  * start kafka:cd /Users/wangleigis163.com/Documents/alex/dev/evn/kafka_2.12-2.2.0 && nohup  bin/kafka-server-start.sh config/server.properties &
  * create topic:./bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181  --replication-factor 1 --partitions 1 --topic spark_topic_1
  * send data to kafka:./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic spark_topic_1
  * example data

 {"eventtimestamp":"2020-08-16 12:02:00","guid":"1","name":"cat", "age":1}
 {"eventtimestamp":"2020-08-16 12:02:00","guid":"2","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:03:00","guid":"3","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:02:00","guid":"1","name":"dog", "age":1} 会去重
{"eventtimestamp":"2020-08-16 12:03:00","guid":"1","name":"dog", "age":1}  不会去重
{"eventtimestamp":"2020-08-16 12:03:00","guid":"4","name":"owl", "age":1}



  *
  */
object StructStreamingDeduplicationOperation {
  def main(args: Array[String]): Unit = {
    kafka_source
  }
  def kafka_source:Unit={
    val spark = SparkSession
      .builder
      .appName("StructStreamingKafka")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark_topic_1")
      .load()

    import spark.implicits._

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    df.printSchema()
    import org.apache.spark.sql.functions

    val schema = StructType {
      List(
        StructField("eventtimestamp", TimestampType, true),
        StructField("guid",StringType),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }


    //获取kafka 中的value并解析处理
    val kafka_value_stream = df
      .where("topic = 'spark_topic_1'")
      .select(functions.from_json(functions.col("value").cast("string"), schema).alias("parsed_value"))
      .select("parsed_value.eventtimestamp","parsed_value.guid", "parsed_value.name", "parsed_value.age")

    val windowedCounts = kafka_value_stream
     // .dropDuplicates("guid") //没有watermark的去重
        .withWatermark("eventtimestamp", "10 minutes")
       .dropDuplicates("guid", "eventtimestamp")//有watermark的去重
      .groupBy(
      window($"eventtimestamp",
        "10 minutes", "5 minutes"),
      $"name"
    ).count()



    val query = windowedCounts.writeStream.
      trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("complete")
    //  .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    query.awaitTermination()


  }



}
