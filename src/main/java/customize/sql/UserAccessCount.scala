package customize.sql

import org.apache.spark.sql.SparkSession

/**
 * @author wangguochao
 * @date 2023/11/29 15:22
 * @description 统计每个用户的累计访问次数
 *
 * https://www.lilinchao.com/archives/1620.html
 */
object UserAccessCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("UserAccessCount").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 读取 csv 数据
    val df = spark.read
      .option("header", true)
      .csv("./src/main/resources/spark_sql/user_access_count.csv")

    // 创建临时视图
    df.createTempView("UserAccess")

    // 1.修改数据格式，将时间按照月份进行转换（根据需求进行格式化处理）
    spark.sql(
      """
        | select
        |   userid,
        |   date_format(regexp_replace(visitdate, '/', '-'), 'yyyy-MM') date,
        |   visitcount
        | from
        |   UserAccess
        |""".stripMargin)
//      .show()
      .createTempView("UserAccessDataChange")

    // 2.根据每个用户每个月的访问量进行聚合统计
    spark.sql(
      """
        | select
        |   userid,
        |   date,
        |   sum(visitcount) as usercount
        | from UserAccessDataChange
        | group by userid, date
        | order by userid, date
        |""".stripMargin)
//      .show()
      .createTempView("UserAccessGroup")

    spark.sql("select * from UserAccessGroup").show()

    // 3.按月累计计算访问量
    // 用一个 sum 开窗函数，对 userid 进行分组，在通过 date 时间进行排序
    spark.sql(
      """
        | select
        |   userid,
        |   date,
        |   usercount,
        |   sum(usercount) over (partition by userid order by date asc) as totalcount
        | from UserAccessGroup
        | order by userid, date
        |""".stripMargin)
      .show()

    // 关闭连接
    spark.stop()
  }

}
