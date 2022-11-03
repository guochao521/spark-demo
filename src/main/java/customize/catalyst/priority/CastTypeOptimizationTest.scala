package customize.catalyst.priority

import org.apache.spark.sql.SparkSession

/**
 * @author wangguochao
 * @date 2022/9/14
 */

/**
 * 链接： https://www.modb.pro/db/444775
 */
object CastTypeOptimizationTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .getOrCreate()

    val df = spark.read.json("./src/main/resources/sql_test.json")
    df.show()
    df.createOrReplaceTempView("sql_test")

    spark.sql("select * from sql_test where f1 > 3.0").show()
    println(spark.sql("select * from sql_test where f1 > 3").queryExecution.optimizedPlan.numberedTreeString)

    /**
     * 3
     * 00 Filter (isnotnull(f1#7) AND (cast(f1#7 as int) > 3))
       01 +- Relation [f1#7,f2#8,id#9] json
     */

    /**
     * 3.0
     * 00 Filter (isnotnull(f1#7) AND (cast(f1#7 as double) > 3.0))
       01 +- Relation [f1#7,f2#8,id#9] json
     */

    // spark的类型提升会依赖于条件中数据类型。不同的执行计划最后得到的结果也不一样。


    // 添加 Cast 优化器
    spark.experimental.extraOptimizations = Seq(CastTypeOptimization)
    println(spark.sql("select * from sql_test where f1 > 3").queryExecution.optimizedPlan.numberedTreeString)

  }

}
