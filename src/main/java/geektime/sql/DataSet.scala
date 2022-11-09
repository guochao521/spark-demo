package geektime.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataSet {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {

    val basePath = "./src/main/resources/"
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLSimple")
      .config("truncate","false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Encoders for most common types are automatically provided by importing spark.implicits._
   //基本类型，自动提供序列化
    import spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    println(primitiveDS.map(_ + 1).collect().mkString("Array(", ", ", ")")) // Returns: Array(2, 3, 4)

    // Encoders are created for case classes
    val caseClassDS =  Seq(Person("alex", 32)).toDS()
    caseClassDS.show()
    // +----+---+
    // |name|age|
    // +----+---+
    // |alex| 32|
    // +----+---+
    val ds_json = spark.read.json(basePath + "people.json")
    ds_json.printSchema()

    /**
    root
   |-- age: long (nullable = true)
   |-- name: string (nullable = true)
   |-- time: string (nullable = true)
   |-- time1: string (nullable = true)
      */
    ds_json.show()
    //*************************************模式推*********************************************
    ds_json.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val filterDF = spark.sql("SELECT name, age FROM people WHERE age>20")
    filterDF.printSchema()

    /**
    root
     |-- name: string (nullable = true)
     |-- age: long (nullable = true)
      */
    filterDF.show()


    val schema = StructType {
      List(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("time", StringType, true)
      )
    }

    val ds_json_1 = spark.read.schema(schema).json(basePath + "people.json")
    ds_json_1.printSchema()
    ds_json_1.show()

  }
}
