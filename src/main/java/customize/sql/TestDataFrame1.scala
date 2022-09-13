package customize.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class People(var name:String, var age:Int)

object TestDataFrame1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)

    // 将本地的数据读入RDD，并将RDD与case class 关联
    val peopleRDD = sc.textFile("F:\\tmp\\SQL_test\\people.txt").map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))

    // 将RDD转换成DataFrames
    import context.implicits._
    val df = peopleRDD.toDF

    // 将DataFrame创建成一个临时的视图
    df.createOrReplaceTempView("people")
    // 使用SQL语句进行查询
    context.sql("select * from people").show()
  }

}
