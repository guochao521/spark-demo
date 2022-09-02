package geektime.connecter

import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * start zookeeper :cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-zookeeper-3.5.5-bin && bin/zkServer.sh start
  * start kafka:cd /Users/wangleigis163.com/Documents/alex/dev/evn/kafka_2.12-2.2.0 && nohup  bin/kafka-server-start.sh config/server.properties &
  * create topic:./bin/kafka-topics.sh --create --zookeeper 127.0.0.1:2181  --replication-factor 1 --partitions 1 --topic spark_topic_1
  * send data to kafka:./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic spark_topic_1
  * example data
  * {"name":"Justin", "age":19,"time":"2019-06-22 01:45:52.478","time1":"2019-06-22 02:45:52.478"}
  * *
  * cp /Users/wangleigis163.com/.m2/repository/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar /Users/wangleigis163.com/Documents/alex/dev/evn/spark-3.0.0-bin-hadoop2.7/jars
  * cp /Users/wangleigis163.com/.m2/repository/org/apache/spark/spark-streaming_2.12/3.0.0/spark-streaming-kafka-0-10_2.12-3.0.0.jar /Users/wangleigis163.com/Documents/alex/dev/evn/spark-3.0.0-bin-hadoop2.7/jars
  * cp /Users/wangleigis163.com/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.0/spark-token-provider-kafka-0-10_2.12-3.0.0.jar /Users/wangleigis163.com/Documents/alex/dev/evn/spark-3.0.0-bin-hadoop2.7/jars
  * cd $SPARK_HOME && nohup ./bin/spark-submit  --class "geekbang.quickstart.SparkStreamingDemo" --master  yarn --deploy-mode cluster /Users/wangleigis163.com/Documents/alex/dev/code/private/system-architecture/spark/target/spark-1.0.jar &
  * reduceByKey(_+_)是reduceByKey((x,y) => x+y)的一个 简洁的形式
  * bin/hdfs dfs -rm -r /spark/etl/
  * bin/hdfs dfs -mkdir /spark/etl/
  *
  * /**
  * * ./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic spark_topic_1
  **/
  *
  */
object SparkKafka {
  def main(args: Array[String]): Unit = {
     initProducer()
   // initConsumer()
  }


  def initConsumer(): Unit = {
    try {
      //http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark_stream_cg",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("spark_topic_1", "spark_topic_2")
      val conf = new SparkConf().setAppName("SparkStreamingDemo")
        .setMaster("local")
      //.setMaster("yarn")
      val streamingContext = new StreamingContext(conf, Seconds(30))
      val stream = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      val kafkaMessage = stream.map(record => (record.value().toString))
        .filter(message => None != JSON.parseFull(message)).print()
      streamingContext.start()
      streamingContext.awaitTermination()
    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }

    }
  }

  def initProducer(): Unit = {

    try {
      // 初始化KafkaSink,并广播
      val conf = new SparkConf()
        .setAppName("ScalaKafkaStream")
        .setMaster("local[3]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("WARN")
      val bootstrapServers = "127.0.0.1:9092"
      val topicName = "spark_topic_1"
      // 初始化KafkaSink,并广播
      val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
        val kafkaProducerConfig = {
          val p = new Properties()
          p.setProperty("bootstrap.servers", bootstrapServers)
          p.setProperty("key.serializer", classOf[StringSerializer].getName)
          p.setProperty("value.serializer", classOf[StringSerializer].getName)
          p
        }
        sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
      }
      val basePath = "./src/main/resources/"
     val localMessageRDD = sc.textFile(basePath+ "message.txt")
      localMessageRDD.foreachPartition( rdd =>{
        if(!rdd.isEmpty){
          rdd.foreach( record => {
            kafkaProducer.value.send(topicName, record)//消息发送
            println("send Message %s".format(record))
          })
        }
      })

    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }

    }

  }


}
