package customize.experiment.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/2/16 17:48
 */

/**
 * 方式一：通过 case class 创建 DataFrames（反射）
 */
case class People(var name: String, var age:Int)

object TestDataFrame1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDDToDataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)

    // 将本地数据读入 RDD，并将 RDD 与 case class 关联
    val peopleRDD = sc.textFile("./src/main/resources/people.txt")
      .map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))

    import context.implicits._

    // 将 RDD 转换成 DataFrames
    val df = peopleRDD.toDF()

    // 将 DataFrames 创建一个临时的视图
    df.createOrReplaceTempView("people")

    // 使用 SQL 语句进行进行查询
    context.sql("select * from people").show()
  }
}
