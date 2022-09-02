package geektime.streaming

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
/**
  * start zookeeper :cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-zookeeper-3.5.5-bin && bin/zkServer.sh start
  * start kafka:cd /Users/wangleigis163.com/Documents/alex/dev/evn/kafka_2.12-2.2.0 && nohup  bin/kafka-server-start.sh config/server.properties &
  * create topic:./bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181  --replication-factor 1 --partitions 1 --topic spark_topic_1
  * send data to kafka:./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic spark_topic_1
  * example data

 {"eventtimestamp":"2020-08-16 12:02:00","name":"cat", "age":1}
 {"eventtimestamp":"2020-08-16 12:02:00","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:03:00","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:03:00","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:07:00","name":"owl", "age":1}
 {"eventtimestamp":"2020-08-16 12:07:00","name":"cat", "age":1}
 {"eventtimestamp":"2020-08-16 12:11:00","name":"dog", "age":1}
 {"eventtimestamp":"2020-08-16 12:13:00","name":"owl", "age":1}


  {"eventtimestamp":"2020-08-16 12:04:00","name":"dog", "age":1}
  {"eventtimestamp":"2020-08-16 11:04:00","name":"cat", "age":1}
   {"eventtimestamp":"2020-08-16 12:14:00","name":"dog", "age":1}

  {"eventtimestamp":"2020-08-16 12:34:00","name":"dog", "age":1}

  *************************TimestampType sample data***********************************

 {"eventtimestamp":"1597550520000","name":"cat", "age":1}
 {"eventtimestamp":"1597550520000","name":"dog", "age":1}
 {"eventtimestamp":"1597550580000","name":"dog", "age":1}
 {"eventtimestamp":"1597550580000","name":"dog", "age":1}
 {"eventtimestamp":"1597550820000","name":"owl", "age":1}
 {"eventtimestamp":"1597550820000","name":"cat", "age":1}
 {"eventtimestamp":"1597551060000","name":"dog", "age":1}
 {"eventtimestamp":"1597551180000","name":"owl", "age":1}

  过期数据
  {"eventtimestamp":"1597550640000","name":"dog", "age":1}
{"eventtimestamp":"1597581704332","name":"dog", "age":1}
  *
  */
object StructStreamingWindowOperation {
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
        //StructField("eventtimestamp", StringType, true),
        StructField("eventtimestamp", TimestampType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }


    //获取kafka 中的value并解析处理
    val kafka_value_stream = df
      .where("topic = 'spark_topic_1'")
      .select(functions.from_json(functions.col("value").cast("string"), schema).alias("parsed_value"))
      .select("parsed_value.eventtimestamp","parsed_value.name", "parsed_value.age")


    val windowedCounts = kafka_value_stream
      .withWatermark("eventtimestamp", "10 minutes")
      .groupBy(
      window($"eventtimestamp",
        "10 minutes",
        "5 minutes"),
      $"name"
    ).count()
      //.orderBy("window")

    val query = windowedCounts.writeStream
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
    //  .outputMode("complete")
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()

    query.awaitTermination()


  }



}
