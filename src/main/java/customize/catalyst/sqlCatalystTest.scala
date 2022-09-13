package customize.catalyst

import org.apache.spark.sql.SparkSession

/**
 * @author wangguochao
 * @date 2022/9/13
 */
object sqlCatalystTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/resources/people.csv")

    df.toDF().write.saveAsTable("person")

    // scalastyle: off
    println(spark.sql("select age * 1 from person").queryExecution.optimizedPlan.numberedTreeString)
    // scalastyle: on
    spark.stop()
  }
}
