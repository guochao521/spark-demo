package geektime.sql

import org.apache.spark.sql.SparkSession
//https://www.cnblogs.com/itboys/p/9215594.html
object SQLOnHive {
  def main(args: Array[String]): Unit = {
    val basePath = "./src/main/resources/"
    //1:初始化SparkContext
    val masterURL = "spark://wangleigis163comdeMacBook-Pro.local:7077"
    val spark = SparkSession
      .builder()
      .master(masterURL)
      .master("local")
      .appName("SQLOnHive")
//      .config("truncate","false")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql
   // spark.sql("show tables").show()
    spark.sql(" CREATE TABLE persion(name STRING,age INT)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n';")


  }
}
