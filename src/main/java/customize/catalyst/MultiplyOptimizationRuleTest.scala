package customize.catalyst

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @author wangguochao
 * @date 2022/9/13
 */
object MultiplyOptimizationRuleTest {

  def main(args: Array[String]): Unit = {
    /**
     * 第一部分：不启用自定义优化规则
     */

    val spark1 = SparkSession.builder
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val df1 = spark1.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/resources/people.csv")

    df1.toDF().write.saveAsTable("person_sql")

    // scalastyle: off
    println(spark1.sql("select age * 1 from person_sql").queryExecution.optimizedPlan.numberedTreeString)

//    /**
//     * 执行结果
//     * // 00 Project [(cast(age#32 as double) * 1.0) AS (age * 1)#34]
//       // 01 +- Relation default.person_sql[name#31,age#32,job#33] parquet
//     *执行计划包括两个部分：
//      01 Relation - 标示我们通过csv文件创建的表。
//      00 Project - 标示Project投影操作。
//      由于age字段默认是string类型，可以看到spark默认会cast成double类型，然后乘以1.0.
//     */
//    // scalastyle: on
//    spark.stop()

    /**
     * 第二部分：启用自定义的优化规则
     */
    // 在 SparkSession 中启用自定义 Optimizer 规则
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    val extensionsBuilder: ExtensionsBuilder = {e => e.injectOptimizerRule(MultiplyOptimizationRule)}

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(extensionsBuilder)
      .getOrCreate()

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("./src/main/resources/people.csv")

    df.toDF().write.saveAsTable("person_rule")
    println(spark.sql("select age * 1 from person_rule").queryExecution.optimizedPlan.numberedTreeString)

    /**
     * 优化后执行结果：可以看到新的执行计划没有了乘以1.0的步骤
     *
     * 00 Project [cast(age#32 as double) AS (age * 1)#34]
       01 +- Relation default.person_rule[name#31,age#32,job#33] parquet
     */

    spark.stop()
  }
}
