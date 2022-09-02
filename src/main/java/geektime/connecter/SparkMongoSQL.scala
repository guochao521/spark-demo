package geektime.connecter

import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object SparkMongoSQL {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    /* For Self-Contained Scala Apps: Create the SparkSession
     * CREATED AUTOMATICALLY IN spark-shell */
    val  mongoURl = "mongodb://127.0.0.1/spark_test.test_collection"
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spark_test.test_collection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spark_test.test_collection")
      .getOrCreate()

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document



//    val docs = """
//      {"name": "Bilbo Baggins", "age": 50}
//      {"name": "Gandalf", "age": 1000}
//      {"name": "Thorin", "age": 195}
//      {"name": "Balin", "age": 178}
//      {"name": "Kíli", "age": 77}
//      {"name": "Dwalin", "age": 169}
//      {"name": "Óin", "age": 167}
//      {"name": "Glóin", "age": 158}
//      {"name": "Fíli", "age": 82}
//      {"name": "Bombur"}""".trim.stripMargin.split("[\\r\\n]+").toSeq
//
    // 需要添加隐式转换
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    val docs= List(("name"->"alex"),("name"->"vic")).map(x=>write(x))

    sparkSession.sparkContext.parallelize(docs.map(Document.parse)).saveToMongoDB()

    // Additional operations go here...

    val df = MongoSpark.load(sparkSession)  // Uses the SparkSession
    df.printSchema()                        // Prints DataFrame schema


    val df2 = sparkSession.sparkContext.loadFromMongoDB()// SparkSession used for configuration
    val df3 = sparkSession.sparkContext.loadFromMongoDB(ReadConfig(
      Map("uri" -> mongoURl)
    )
    ) // ReadConfig used for configuration

    df.filter(df("age") < 100).show()


    val rdd_data = MongoSpark.load(sparkSession)
    rdd_data.createOrReplaceTempView("characters")

    val rdd_data_sql = sparkSession.sql("SELECT name, age FROM characters WHERE age >= 100")
    rdd_data_sql.show()

  }
}
