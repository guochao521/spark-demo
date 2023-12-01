package customize.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangguochao
 * @date 2023/11/30 11:49
 *
 * 统计有过连续3天以上销售的店铺,并计算销售额
 * https://www.lilinchao.com/archives/1623.html
 */
object RollupMthIncomeSQL   {

  """
  步骤：
    1.将每天的金额求和（同一天可能会有多个订单）
    2.给每个商家中每日的订单按时间排序并打上编号
    3.获取date与rn的差值的字段
    4.获得最终结果
  """

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("./src/main/resources/spark_sql/order.csv")

    df.createOrReplaceTempView("v_orders")

    // 1.将每天的金额求和（同一天可能会有多个订单）
    spark.sql(
      """
        | select
        |   sid,
        |   datetime,
        |   SUM(money) day_money
        | from v_orders
        | group by sid, datetime
        |""".stripMargin)
//      .show()
      .createTempView("t1")

    // 2.给每个商家中每日的订单按时间排序并打上编号
    spark.sql(
      """
        | select
        |   sid,
        |   datetime,
        |   day_money,
        |   ROW_NUMBER() over (PARTITION BY sid ORDER BY datetime) rn
        | from t1
        |""".stripMargin)
      .createTempView("t2")

    // 3.获取date与rn的差值的字段
    spark.sql(
      """
        | select
        |   sid,
        |   datetime,
        |   day_money,
        |   DATE_SUB(datetime, rn) diff
        | from t2
        |""".stripMargin)
      .createTempView("t3")

    // 4.获得最终结果
    spark.sql(
      """
        | select
        |   sid,
        |   MIN(datetime) begin_date,
        |   MAX(datetime) end_date,
        |   COUNT(*) times,
        |   SUM(day_money) total_sales
        | from t3
        | group by sid, diff
        | HAVING times >= 3
        |""".stripMargin)
      .show()
  }

}
