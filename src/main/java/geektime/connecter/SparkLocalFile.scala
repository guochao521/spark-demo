package geektime.connecter

import org.apache.spark.sql.SparkSession

object SparkLocalFile {
  def main(args: Array[String]): Unit = {
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
    df_text.show()


    df_text.repartition(2).rdd.glom().collect().foreach(x=>  println(x.getClass.getName+" partions length"+x.length))
    println("numPartitions:"+df_text.repartition(3).rdd.getNumPartitions)
   // df_text.repartition(2).foreachPartition()

    //  write.text(basePath + filePrex + "alex-text_result/")

  }

}
