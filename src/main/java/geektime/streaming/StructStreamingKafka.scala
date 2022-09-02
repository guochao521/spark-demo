package geektime.streaming

import java.util.concurrent.TimeUnit

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * start zookeeper :cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-zookeeper-3.5.5-bin && bin/zkServer.sh start
  * start kafka:cd /Users/wangleigis163.com/Documents/alex/dev/evn/kafka_2.12-2.2.0 && nohup  bin/kafka-server-start.sh config/server.properties &
  * create topic:./bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181  --replication-factor 1 --partitions 1 --topic spark_topic_1
  * send data to kafka:./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic spark_topic_1
  * example data
  * {"name":"Justin", "age":19}
  *
  * ./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic spark_topic_2
  *
  */
object StructStreamingKafka {
  def main(args: Array[String]): Unit = {
    kafka_source
 //   kafka_writer


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
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions



    /*
        简单获取kafka数据源
       val query = df.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
       .outputMode("append")
       .format("console")
       .start()
 */

    //获取kafka 中的value并解析处理
    val kakfa_value_stream = df
      .where("topic = 'spark_topic_1'")
      .select(functions.from_json(functions.col("value").cast("string"), schema).alias("parsed_value"))
      .select("parsed_value.name", "parsed_value.age")

    val query = kakfa_value_stream.writeStream.trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()

  }

  def kafka_writer :Unit={
    val spark = SparkSession
      .builder
      .appName("StructStreamingKafka")
      .master("local")
      .getOrCreate()

    val basePath = "./src/main/resources/"
    val schema = StructType {
      List(
        StructField("key", StringType, true),
        StructField("value", StringType, true)
      )
    }




    import spark.implicits._
    val fileSource = spark.read
      .schema(schema)
      .format("json")
      .load(basePath + "structured_dir_1")
      .filter("key >= 2")


    fileSource.printSchema()
    fileSource.show(100)
    fileSource
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
       .write
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("topic", "spark_topic_2")
       .save()


  }

}
