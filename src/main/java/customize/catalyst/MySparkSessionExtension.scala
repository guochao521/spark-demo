package customize.catalyst

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @author wangguochao
 * @date 2022/9/14
 */
object MySparkSessionExtensionTest {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .config("spark.sql.extensions", "customize.catalyst.MySparkSessionExtension")
      .getOrCreate()

    val df = sparkSession.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/resources/people.csv")

    df.createOrReplaceTempView("person_rule")
    println(sparkSession.sql("select age * 1 from person_rule").queryExecution.optimizedPlan.numberedTreeString)
  }

}


class MySparkSessionExtension  extends ((SparkSessionExtensions) => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new MySqlParser(session, parser)
    }
  }
}