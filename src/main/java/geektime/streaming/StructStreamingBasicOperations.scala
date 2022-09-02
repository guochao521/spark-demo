package geektime.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.sparkproject.guava.collect.ImmutableMap

/**
  * start zookeeper :cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-zookeeper-3.5.5-bin && bin/zkServer.sh start
  * start kafka:cd /Users/wangleigis163.com/Documents/alex/dev/evn/kafka_2.12-2.2.0 && nohup  bin/kafka-server-start.sh config/server.properties &
  * create topic:./bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181  --replication-factor 1 --partitions 1 --topic spark_topic_1
  * send data to kafka:./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic spark_topic_1
  * example data
  * {"name":"Justin", "age":19}
  *
  *
  */
object StructStreamingBasicOperations {
  def main(args: Array[String]): Unit = {
    kafka_source
  }
  def kafka_source:Unit={
    val spark = SparkSession
      .builder
      .appName("StructStreamingKafka")
      .master("local")
      .getOrCreate()

    val schema = StructType {
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    }

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

    //获取kafka 中的value并解析处理
    val kafka_value_stream = df
      .where("topic = 'spark_topic_1'")
      .select(functions.from_json(functions.col("value").cast("string"), schema).alias("parsed_value"))
      .select("parsed_value.name", "parsed_value.age")

//    val people_ds :Dataset[People]=  kafka_value_stream.encoder.schema(kafka_value_stream.schema)
//    //select 操作
//    import spark.implicits._
  /* val sql_filter = kafka_value_stream.select("name").where("age > 2")
   val select_filter = kafka_value_stream.filter(x => x.getAs[Int]("age")>2)
   val select_filter_2 = kafka_value_stream.filter("age > 2")

    val query = group_by_name.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("append")
      .format("console")
      .start()*/


    //val group_by_name = kafka_value_stream.groupBy("name").count()
    //val group_by_name = kafka_value_stream.groupBy("name").agg(ImmutableMap.of("age", "avg"))


    kafka_value_stream.createOrReplaceTempView("view_people")
    val view_people = spark.sql("select name,count(name) from view_people group by name")  // returns another streaming DF

    val query = view_people.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }


}
