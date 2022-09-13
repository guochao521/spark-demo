package customize.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object TestDataFrame2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val fileRDD = sc.textFile("F:\\tmp\\SQL_test\\people.txt")

    // 将 RDD 数据映射成 ROW，需要 import org.apache.spark.sql.Row
    val rowRDD: RDD[Row] = fileRDD.map( line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })

    // 创建 StructType 来定义结构
    val stuctType: StructType = StructType(
      // 字段名，字段类型，是否可以为空
      StructField("name", StringType, true)::
        StructField("age", IntegerType, true)::Nil
    )

    /**
     * rows: java.util.List[Row],
     * schema: StructType
     */
    val df:DataFrame = sqlContext.createDataFrame(rowRDD, stuctType)
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()

    val schema = new StructType().add("name", new StructType().add("firstname", StringType))
  }

}
