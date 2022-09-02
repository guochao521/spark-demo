package geektime.optimize

import com.esotericsoftware.kryo.Kryo
import org.apache.spark._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object OptimizeSQL {



  def main(args: Array[String]): Unit = {
    val basePath = "./src/main/resources/"
    //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLSimple")
      .config("truncate","false")
      .config("spark.sql.files.maxPartitionBytes","")
      .getOrCreate()

    val df_json = spark.read.json(basePath + "people.json")
    df_json.show()

    df_json.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people where age > 20")
    sqlDF.show()


  //  Thread.sleep(10000000)
  }

}
