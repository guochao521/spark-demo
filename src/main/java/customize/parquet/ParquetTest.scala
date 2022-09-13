package customize.parquet

import org.apache.spark.sql.SparkSession

/**
 * @author wangguochao
 * @date 2022/7/21
 */
object ParquetTest {

  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("ParquetTest").setMaster("local")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("INFO")
//    val spark = new SQLContext(sc)

    val spark: SparkSession = SparkSession.builder()
      .appName(ParquetTest.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    // 构建数据
    val data = Seq(("James","","Smith","36636","M",3000),
      ("Michael","Rose","","40288","M",4000),
      ("Robert","","Williams","42114","M",4000),
      ("Maria","Anne","Jones","39192","F",4000),
      ("Jen","Mary","Brown","","F",-1))

    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    // 导入 implicit DF,DS
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)
    df.show()

    // 使用 DataFrameWriter 类的 parquet() 函数，将 Spark DataFrame 写入 Parquet 文件
    df.write.parquet("./output/people.parquet")
  }
}
