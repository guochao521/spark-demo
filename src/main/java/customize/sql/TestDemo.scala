package customize.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

/**
 * @author wangguochao
 * @date 2023/11/29 21:58
 * 面试题
 */
object TestDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TestDemo").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 读取 csv 数据
    val df = spark.read
      .option("header", false)
      .csv("./src/main/resources/spark_sql/test_data.csv")
      .toDF("province_name", "city_name", "pc_cnt")

    df.withColumn("pc_cnt",col("pc_cnt")
      .cast(LongType)).createTempView("b_province")

//    df.selectExpr("cast(pc_cnt as long) pc_cnt")

    df.printSchema()

    spark.sql("select * from b_province").show()

    //        |

    spark.sql(
      """
        | select
        |   province_name,
        |   concat_ws(',', collect_set(city_name)) as top2_city
        | from (
        |   select
        |     province_name,
        |     city_name,
        |     row_number() over (partition by province_name order by pc_cnt desc) rn
        |   from b_province
        | ) t
        | where rn <= 2
        | group by province_name
        |"""
        .stripMargin)
        .show()
  }

}
