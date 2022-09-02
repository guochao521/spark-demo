package geektime.dataformat

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkDataFormat {
  def main(args: Array[String]): Unit = {
    try {
      val basePath = "./src/main/resources/"
      //1:初始化SparkContext
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("SparkDataFormat")
        .config("spark.default.parallelism",3)
        .getOrCreate()

      val filePrex = System.currentTimeMillis() + "-"

      val df_text = spark.read.text(basePath + "kv1.txt")
      print("df_text:"+df_text.rdd.getNumPartitions)
      df_text.show()
      df_text.write.text(basePath + filePrex + "text_result")

      val df_csv = spark.read.format("csv").option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true") //将第一行进行检查并推断其scheme
        .load(basePath + "people.csv")

      df_csv.show()
      df_csv.write.csv(basePath + filePrex + "csv_result")

      val df_json = spark.read.json(basePath + "people.json")
      df_json.show()
      df_json.write.json(basePath + filePrex + "json_result")


      val df_parquet = spark.read.parquet(basePath + "users.parquet")
      print("df_parquet:"+df_parquet.rdd.getNumPartitions)
      df_parquet.show()
      df_parquet.write.parquet(basePath + filePrex + "parquet_result")

      val df_orc = spark.read.orc(basePath + "users.orc")
      df_orc.show()
      df_orc.write.orc(basePath + filePrex + "orc_result")

      val df_avro = spark.read.format("avro").load(basePath + "users.avro")
      println("***********AVRO***************")
      df_avro.show()
      df_avro.write.format("avro").save(basePath + filePrex + "avro_result")

    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
