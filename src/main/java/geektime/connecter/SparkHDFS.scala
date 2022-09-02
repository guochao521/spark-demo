package geektime.connecter

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkHDFS {
  def main(args: Array[String]): Unit = {
     //1:初始化SparkContext
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkDataFormat")
      .getOrCreate()

    val hdfsSourcePath = "hdfs://127.0.0.1:9000/input/people.json"

    val filePrex = System.currentTimeMillis() + "-"
    val df = spark.read.json(hdfsSourcePath)
    df.show()
    val hdfsTargetPath = "hdfs://127.0.0.1:9000/input/"+filePrex+"+people_result.json"
    df.write.mode(SaveMode.Overwrite) json (hdfsTargetPath)

  }

}
