package geektime.connecter


import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMysql {
  val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"

  def main(args: Array[String]): Unit = {
    //readMysqlFirstWay()
    // readMysqlSecondWay()
    // writeMysqlFirstWay()
    writeMysqlSecondWay()
  }

  /**
    * spark.read.jdbc()
    */
  def readMysqlFirstWay(): Unit = {
    val spark = SparkSession.builder().appName("sparksql").master("local").getOrCreate()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")

    val dataFrame = spark.read.jdbc(url, "user", prop).select("id").where("id = 1").show()

    spark.stop()
  }

  /**
    * spark.read.format().option().load()
    */
  def readMysqlSecondWay(): Unit = {

    val spark = SparkSession.builder()
      .appName("sparksql")
      .master("local")
      .getOrCreate()

    //useUnicode=true&characterEncoding=UTF-8 编码
    //serverTimezone=UTC 时区
    val dataDF = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "user")
      .option("user", "root")
      .option("password", "root")
      .load()

    dataDF.createOrReplaceTempView("tmptable")

    val sql = "select * from tmptable where id = 1"

    spark.sql(sql).show()
    spark.stop()
  }

  /**
    * 查询后写入
    *
    * @param args
    */
  def writeMysqlFirstWay(): Unit = {
    val spark = SparkSession.builder().appName("sparksql").master("local").getOrCreate()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")


    val dataFrame = spark.read.jdbc(url, "user", prop).where("id = 1")

    dataFrame.write.mode(SaveMode.Append).jdbc(url, "user_1", prop)

    spark.stop()
  }


  def writeMysqlSecondWay(): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")


    val basePath = "./src/main/resources/"


    val userDF = spark.read.format("csv").option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true") //将第一行进行检查并推断其scheme
      .load(basePath + "user.csv")


    val schema = StructType {
      List(
        StructField("id", StringType, true),
        StructField("name", StringType, true),
        StructField("email", StringType, true)
      )
    }


    // userDF.write.mode(SaveMode.Overwrite).jdbc(url,"user",prop)

    val userDFFilter = userDF.filter(" id >2")

    userDFFilter.write.mode(SaveMode.Overwrite).jdbc(url, "user_1", prop)

    spark.stop()
  }

}
