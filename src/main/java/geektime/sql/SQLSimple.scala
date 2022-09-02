package geektime.sql

import org.apache.spark.sql.SparkSession

object SQLSimple {
  def main(args: Array[String]): Unit = {
    val basePath = "./src/main/resources/"
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLSimple")
      .config("truncate","false")
      .getOrCreate()

    val df_json = spark.read.json(basePath + "people.json")
    df_json.show()

    /**
    +----+-------+--------------------+--------------------+
    | age|   name|                time|               time1|
    +----+-------+--------------------+--------------------+
    |null|Michael|2019-06-22 01:45:...|2019-06-22 02:45:...|
    |  30|   Andy|2019-06-22 01:45:...|2019-06-22 02:45:...|
    |  19| Justin|2019-06-22 01:45:...|2019-06-22 02:45:...|
    |  19|   alex|2019-06-22 01:45:...|2019-06-22 02:45:...|
    +----+-------+--------------------+--------------------+
      */

    println(df_json.schema)
    /*
StructType(StructField(age,LongType,true),
StructField(name,StringType,true), StructField(time,StringType,true), StructField(time1,StringType,true))
     */
    df_json.select("name","time").show()
    /**
      * +-------+--------------------+
      * |   name|                time|
      * +-------+--------------------+
      * |Michael|2019-06-22 01:45:...|
      * |   Andy|2019-06-22 01:45:...|
      * | Justin|2019-06-22 01:45:...|
      * |   Andy|2019-06-22 01:45:...|
      * +-------+--------------------+
      */

    df_json.groupBy("name").count().show()

    /**
    +-------+-----+
    |   name|count|
    +-------+-----+
    |Michael|    1|
    |   Andy|    2|
    | Justin|    1|
    +-------+-----+
      */

    df_json.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people where age > 20").show()

    df_json.createOrReplaceGlobalTempView("people")
    spark.sql("SELECT * FROM people where age>=20").explain(true)
    //Thread.sleep(1000000000)

  }
}
