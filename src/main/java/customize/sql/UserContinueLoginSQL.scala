package customize.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangguochao
 * @date 2023/3/31 10:23
 *
 * 统计连续登录三天及以上的用户
 * 链接：https://www.lilinchao.com/archives/1622.html
 */
object UserContinueLoginSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("UserContinueLoginSQL").master("local[1]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/spark_sql/v_user_login.csv")

    df.createOrReplaceTempView("v_access_log")

    // 1. 查询用户登录详情，并且根据访问时间对每个用户访问次数进行累计
    // row_number() 函数将针对 SELECT 语句返回的每一行，从 1 开始编号，赋予其连续的编号
    spark.sql(
      """
        | select
        |   uid,
        |   translate(datatime, '/', '-') datatime,
        |   ROW_NUMBER() over(PARTITION BY uid ORDER BY datatime ASC) rn
        | from v_access_log
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql("select * from t1").show()

//    spark.sql("select date_sub('2018-2-28', 1)").show()

    // 2. 查询出连续登录日期
    /**
     * 思考：如何能确定是否是连续时间登录
     *  - 寻找规律：登录累积次数减登录时间，如果相同则是连续时间登录
     *  DATE_SUB(): 日期相减
     */
    spark.sql(
      """
        | select
        |   uid,
        |   datatime,
        |   DATE_SUB(datatime, rn) dif
        | from t1
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql("select * from t2").show()

    // 3. 对 uid 和 dif 进行分组，统计数量
    spark.sql(
      """
        | select
        |   uid,
        |   MIN(datatime) start_time,
        |   MAX(datatime) end_date,
        |   count(1) counts
        | from t2
        | group by uid, dif HAVING counts >= 3
        |""".stripMargin).show()
  }

  //+------+----------+--------+------+
  //|   uid|start_time|end_date|counts|
  //+------+----------+--------+------+
  //|guid01| 2018-2-28|2018-3-2|     3|
  //|guid01|  2018-3-4|2018-3-7|     4|
  //|guid02|  2018-3-1|2018-3-3|     3|

}
