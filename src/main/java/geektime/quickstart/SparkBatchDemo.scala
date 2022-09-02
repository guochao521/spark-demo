package geektime.quickstart


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.control.Exception


//cd $SPARK_HOME && ./bin/spark-submit  --class "geekbang.quickstart.SparkBatchDemo" --master  yarn --deploy-mode cluster /Users/wangleigis163.com/Documents/alex/dev/code/private/system-architecture/spark/target/spark-1.0.jar
object SparkBatchDemo {
  def main(args: Array[String]) {

    try {

      //1:input data path
      val hdfsSourcePath = "hdfs://127.0.0.1:9000/input/people.json"

      //2:create SparkSession
      val spark = SparkSession
        .builder()
        .appName("SparkBatchDemo")
         .master("local")
        //.master("yarn")
        // .config("spark.some.config.option", "some-value")
        .getOrCreate()
      //3:read data from hdfs
      val df = spark.read.json(hdfsSourcePath)
      //4.1: Displays the content of the DataFrame to stdout
      df.show()
      df.printSchema()
      df.select("name").show()
      //4.2 Select everybody, but increment the age by 1
      import spark.implicits._
      df.select($"name", $"age" + 1).show()

      df.filter($"age" > 21).show()

      df.groupBy("age").count().show()
      //5 write result to hdfs
    //  val hdfsTargetPath = "hdfs://127.0.0.1:9000/input/result/"
      //bin/hdfs dfs -rm -r /input/people_result.json
     // df.write.mode(SaveMode.Overwrite) json (hdfsTargetPath)

    } catch {
      case ex: Exception => {
        ex.printStackTrace() // 打印到标准err
        System.err.println("exception===>: ...") // 打印到标准err
      }
    }
  }
}
