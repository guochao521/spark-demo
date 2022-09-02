package geektime.connecter

import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

/**


https://www.mongodb.com/try/download/community
tar -zxvf mongodb-macos-x86_64-4.4.0.tgz
cd mongodb-macos-x86_64-4.4.0 && sudo cp bin/ /usr/local/bin/
sudo ln -s  /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/bin/ /usr/local/bin/
sudo mkdir -p /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/mongodb
sudo mkdir -p /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/log/mongodb
chmod -R 777  /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/mongodb
chmod -R 777  /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/log/mongodb/
mongod --dbpath  /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/mongodb --logpath  /Users/wangleigis163.com/Documents/alex/dev/evn/mongodb-macos-x86_64-4.4.0/log/mongodb/mongo.log --fork

登录 mongo
use spark_test;
db.createCollection('test_collection')
db.test_collection.find()
db.test_collection.insert({"uid":1,"name":"mongodb","age":30,"date":new Date()})
db.test_collection.find()
db.stats()

  */
object SparkMongo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MongoSparkConnectorIntro ")
      .setMaster("local")//调试
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark_test.test_collection")
      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark_test.test_collection")
    val sc = new SparkContext(conf)
    import org.bson.Document
   //1：数据写入2种方式，使用SparkConf、WriteConfig
    val documents = sc.parallelize((10 to 12).map(i => Document.parse(s"{spark_test: $i}")))
    MongoSpark.save(documents) // Uses the SparkConf for configuration
    documents.saveToMongoDB(WriteConfig(Map("uri" -> "mongodb://127.0.0.1/spark_test.test_collection")))// Uses the WriteConfig
    documents.saveToMongoDB()// Uses the SparkConf for configuration
  //scala Lists不支持直接插入，需要调用asJava转换为Java列表并存储
    val documentsWithList = sc.parallelize(
      Seq(new Document("fruits", List("apples", "oranges", "pears").asJava))
    )
    MongoSpark.save(documentsWithList)
    //2：数据读取2种方式：使用SparkConf、ReadConfig
    val rdd = MongoSpark.load(sc)
    println(rdd.count)
    println(rdd.first.toJson)
    import com.mongodb.spark.config._
    val readConfig = ReadConfig(Map("collection" -> "test_collection","readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
    val customRdd = MongoSpark.load(sc, readConfig)
    println(customRdd.count)
    println(customRdd.first.toJson)
   val loadUseSparkConfig = sc.loadFromMongoDB() // Uses the SparkConf for configuration
    println(loadUseSparkConfig.first().toJson)

   val loadUserReadConfig = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://127.0.0.1/spark_test.test_collection"))) // Uses the ReadConfig
    println(loadUserReadConfig.first().toJson)
    val filteredRdd = rdd.filter(doc => doc.getInteger("spark_test") > 5)
    println(filteredRdd.count)
    println(filteredRdd.first.toJson)
    val aggregatedRdd = rdd.withPipeline(Seq(Document.parse("{ $match: { spark_test : { $gt : 5 } } }")))
    println(aggregatedRdd.count)
    println(aggregatedRdd.first.toJson)

  }
}
