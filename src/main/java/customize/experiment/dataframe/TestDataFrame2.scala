package customize.experiment.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/2/16 19:36
 */

/**
 * 方式二：通过structType 创建DataFrames（编程接口）
 */
object TestDataFrame2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDataFrame2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val fileRDD = sc.textFile("./src/main/resources/people.txt")

    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row

    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })

    // 创建 StructType 来定义结构
    val structType: StructType = StructType(
      // 字段名，字段类型，是否可以为空
      StructField("name", StringType, nullable = true)::
        StructField("age", IntegerType, nullable = true)::Nil
    )

    /**
     * rows: java.util.List[Row],
     * schema: StructType
     */
    val df: DataFrame = sqlContext.createDataFrame(rowRDD, structType)
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}
