package customize.experiment.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/2/16 19:55
 */

/**
 * 方式三：通过 JSON 文件创建 ataFrames
 */
object TestDataFrame3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TestDataFrame3")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("./src/main/resources/people1.json")
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()

    /**
     * 都知道 json，Parquet 也好，它是一个结构化文件，它是有字段的，它是有 Schema 的。
     * 我们通过 sqlContext.read.json 方式把这个文件读上来，这个 json 文件本身是有 Schema 信息的，
     * 所以 DataFrame 会继承 json 结构化的文件的信息。
     */
  }
}
