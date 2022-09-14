package customize.catalyst

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

/**
 * @author wangguochao
 * @date 2022/8/11
 */
object StrictParserTest {
  def main(args: Array[String]): Unit = {

    // 创建扩展点函数
    /**
     * 这里面有两个函数，extensionBuilder函数用于 SparkSession构建，
     */
    type ParseBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionBuilder = SparkSessionExtensions => Unit
    val parseBuilder: ParseBuilder = (_, parser) => new StrictParser(parser)
    val extensionBuilder: ExtensionBuilder = {
      e => e.injectParser(parseBuilder)
    }

    // 在 SparkSession 中启用自定义 Parser
    val spark = SparkSession.builder
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      .withExtensions(extensionBuilder)
      .getOrCreate()

    val sc = spark.sparkContext

//    {"name":"Michael"}
//    {"name":"Andy","age":30}
//    {"name":"Justin","age":19}
    val df = spark.read.json("C:\\Users\\wangguochao\\Desktop\\test_data\\people.json")
    df.show()
    val schema = (new StructType)
      .add("name", StringType, true)
      .add("age", IntegerType, false)

    df.toDF().write.parquet("person")
//    newTable.registerTempTable("people")
//    newTable.write.parquet("person")
//    df.createOrReplaceTempView("person")
    spark.sql("select * from person limit 2").show()
    spark.stop()
  }
}
