package geektime.delta

import io.delta.tables._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DeltaDemo {
  def main(args: Array[String]): Unit = {
    try {
      val basePath = "./src/main/resources/"
      //1:初始化SparkContext
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("DeltaDemo")
       // .config("spark.default.parallelism",3)
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.deltaschema.autoMerge.enabled", "true")
        .getOrCreate()
      spark.sparkContext.setLogLevel("error")
      val data = spark.range(0, 5)
      data.show(10)

      /**
        * 1：创建一个Delta table,
        * 2：将DataFrame数据以 delta format写入
        * 3：同时也可以将parquet, csv, json等转换为delta.
        *data.write.format("parquet").save(basePath+"delta/parquet")
        */
      data.write.format("delta").save(basePath+"delta/delta-table")
      println("***************Create Delta table Done **********************")

      /**
        * 2:更新delta表数据- Update table data
        */
      println("***************overwrite**********************")
      val data_update = spark.range(5, 10)
      data_update.write.format("delta").mode("overwrite").save(basePath+"delta/delta-table")
      data_update.show()

      /**
        * 3:根据条件更新delta表数据
        */

      val deltaTable = DeltaTable.forPath(basePath+"delta/delta-table")

      // 为每个偶数值添加100
      println("***************update:偶数值添加100**********************")
      deltaTable.update(
        condition = expr("id % 2 == 0"),
        set = Map("id" -> expr("id + 100")))
      deltaTable.toDF.show()
      println("***************delete:删除偶数行**********************")
      // 删除偶数行
      deltaTable.delete(condition = expr("id % 2 == 0"))
      deltaTable.toDF.show()
      // Upsert (merge) new data
      val newData = spark.range(0, 20).toDF
      println("***************Upsert 0-23**********************")
      deltaTable.as("oldData")//将原始数据表命名为oldData，并和newData进行merge
        .merge(newData.as("newData"), "oldData.id = newData.id")//
        .whenMatched.update(Map("id" -> col("newData.id")))//当数据存在是更新
        .whenNotMatched.insert(Map("id" -> col("newData.id")))//当数据不存在是插入
        .execute()
      deltaTable.toDF.show()
      /**
        * 使用时间旅行读取旧版本的数据
        *当我的数据执行了覆盖写后，我们可以通过时间旅行，读取Delta table旧版本数据的snapshots数据
        *
        */
      println("***************时间旅行：读取版本3**********************")
      val df = spark.read.format("delta").option("versionAsOf", 3).load(basePath+"delta/delta-table")
      df.show()
      //appendm模式
      df.write.format("delta").mode("append").save(basePath+"delta/delta-table")

      /**
        * Replace table schema
        *.option("overwriteSchema", "true")
        * spark.databricks.deltaschema.autoMerge.enabled is true
        * Automatic schema update
        * .option("mergeSchema", "true")
        */
      df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(basePath+"delta/delta-table")

     } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
