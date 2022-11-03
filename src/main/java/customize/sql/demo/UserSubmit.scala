package customize.sql.demo

import org.apache.spark.sql.SparkSession

/**
 * @author wangguochao
 * @date 2022/9/16
 */
object UserSubmit {

  def main(args: Array[String]): Unit = {
    // 在 SparkSession 中启用自定义 Parser
    val spark = SparkSession.builder
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val sc = spark.sparkContext


    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/resources/user_submit.csv")

    df.toDF().createOrReplaceTempView("user_submit")

    spark.sql("SELECT SUBSTRING_INDEX(profile,\",\",-1) AS gender,\nCOUNT(*) AS number\nFROM user_submit\nGROUP BY gender;").show()
  }

}
