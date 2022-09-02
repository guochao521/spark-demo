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
spark_topic_1
 {"eventtimestamp_1":"2020-08-16 12:02:00","guid_1":"1","name":"alex", "age":20}
 {"eventtimestamp_1":"2020-08-16 12:02:00","guid_1":"2","name":"vic", "age":30}
 {"eventtimestamp_1":"2020-08-16 12:03:00","guid_1":"3","name":"terry", "age":25}

 {"eventtimestamp_1":"2020-08-16 12:03:00","guid_1":"5","name":"terry", "age":25}
   {"eventtimestamp_1":"2020-08-16 12:03:00","guid_1":"16","name":"terry", "age":25}
spark_topic_2
 {"eventtimestamp_2":"2020-08-16 12:02:00","guid_2":"1","city":"shanghai", "job":"dev"}
 {"eventtimestamp_2":"2020-08-16 12:02:00","guid_2":"2","city":"xian", "job":"design"}
 {"eventtimestamp_2":"2020-08-16 12:04:00","guid_2":"3","city":"beijing", "job":"manager"}
 {"eventtimestamp_2":"2020-08-16 12:04:00","guid_2":"4","city":"beijing", "job":"manager"}

 {"eventtimestamp_2":"2020-08-16 12:04:00","guid_2":"5","city":"beijing", "job":"manager"}

  *
  */
object StructStreamingJoin {
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
        StructField("eventtimestamp_1", TimestampType, true),
        StructField("guid_1",StringType),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }


    //获取kafka 中的value并解析处理
    val kafka_value_stream_1 = df
      .where("topic = 'spark_topic_1'")
      .select(functions.from_json(functions.col("value").cast("string"), schema).alias("parsed_value"))
      .select("parsed_value.eventtimestamp_1","parsed_value.guid_1", "parsed_value.name", "parsed_value.age")




    val df_2 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark_topic_2")
      .load()

    import spark.implicits._

    df_2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    df_2.printSchema()
    import org.apache.spark.sql.functions

    val schema_2 = StructType {
      List(
        StructField("eventtimestamp_2", TimestampType, true),
        StructField("guid_2",StringType),
        StructField("city", StringType, true),
        StructField("job", StringType, true)
      )
    }


    //获取kafka 中的value并解析处理
    val kafka_value_stream_2 = df_2
      .where("topic = 'spark_topic_2'")
      .select(functions.from_json(functions.col("value").cast("string"), schema_2).alias("parsed_value"))
      .select("parsed_value.eventtimestamp_2","parsed_value.guid_2", "parsed_value.city", "parsed_value.job")
    val stream1WithWatermark = kafka_value_stream_1.
      withWatermark("eventtimestamp_1", "10 minutes")
    val stream2WithWatermark = kafka_value_stream_2.
      withWatermark("eventtimestamp_2", "20 minutes")
    // Join with event-time constraints
   val joinStream = stream1WithWatermark.join(
      stream2WithWatermark,
      expr("""
    guid_1 = guid_2 AND
    eventtimestamp_2 >= eventtimestamp_1 AND
    eventtimestamp_2 <= eventtimestamp_1 + interval 10 minutes
    """)
     ,joinType = "left"
    )


  // val joinStream =  stream1WithWatermark.join(stream2WithWatermark, stream1WithWatermark("guid_1") ===  stream2WithWatermark("guid_2"))





    val query = joinStream.writeStream.
      trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
     .outputMode("append")
      .option("truncate", "false")
      .format("console")
      .start()

    query.awaitTermination()


  }



}
