package customize.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wangguochao
 * @date 2023/11/30 12:23
 *
 * 统计用户上网流量: 统计用户上网流量，如果两次上网的时间小于10min，就可以rollup(合并)到一起
 * https://www.lilinchao.com/archives/1624.html
 */
object SparkSQLMerge {
  """
    | 步骤：
    | 1.利用lag函数，把start_time和end_time的数据压到下一行
    | 2.计算差值
    | 3.打标记，如果小于10min为0或者是为空 否则为1
    | 4.以uid为分区，再次开窗，用sum计算和
    | 5.以uid和flags分组，聚合得到结果
    |""".stripMargin

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", " ")
      .csv("./src/main/resources/spark_sql/merge.dat")

    df.createOrReplaceTempView("flow_merge")

    // 1.利用lag函数，把start_time和end_time的数据压到下一行
    spark.sql(
      """
        | select
        |   id,
        |   start_time,
        |   end_time,
        |   lag(start_time, 1) over (PARTITION BY id ORDER BY start_time) lag_start_time, -- 把这行之前的数据拿到这一行
        |   lag(end_time) over (PARTITION BY id ORDER BY start_time) lag_end_time, -- 把这行之前的数据拿到这一行
        |   flow
        | from flow_merge
        | ORDER BY id, start_time ASC
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    // 2.计算差值
    spark.sql(
      """
        | select
        |   id,
        |   start_time,
        |   end_time,
        |   lag_start_time,
        |   lag_end_time,
        |   (unix_timestamp(start_time, 'HH:mm:ss') - unix_timestamp(lag_end_time, 'HH:mm:ss')) as seconds,
        |   flow
        | from t1
        |""".stripMargin)
      .createOrReplaceTempView("t2")

    // 3.打标记，如果小于10min为0或者是为空，否则为1
    spark.sql(
      """
        | select
        |   id,
        |   start_time,
        |   end_time,
        |   case when seconds is null then 0
        |   when seconds <= 10 * 60 then 0
        |   else 1
        |   end flag,
        |   flow
        | from t2
        |""".stripMargin)
      .createOrReplaceTempView("t3")

    spark.sql("select * from t3").show()

    // 4.以uid为分区，再次开窗，用sum计算和
    spark.sql(
      """
        | select
        |   id,
        |   start_time,
        |   end_time,
        |   sum(flag) over (partition by id order by start_time) as flags,
        |   flow
        | from t3
        |""".stripMargin)
      .createOrReplaceTempView("t4")

    spark.sql("select * from t4").show()

    // 5.以uid和flags分组，聚合得到结果
    spark.sql(
      """
        | select
        |   id,
        |   min(start_time) start_time,
        |   max(end_time) end_time,
        |   sum(flow) flow
        | from t4
        | group by id, flags
        | order by id, start_time
        |""".stripMargin).show()
  }

}
