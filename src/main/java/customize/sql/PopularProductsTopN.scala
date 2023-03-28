package customize.sql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author wangguochao
 * @date 2023/2/27 10:32
 */

/**
 * 链接：https://www.lilinchao.com/archives/1621.html
 * 步骤：
 * 1. 连接三张表的数据，获取完整的数据（只有点击）
 * 2. 将数据根据地区，商品名称分组
 * 3. 统计商品点击次数总和,取 Top3
 * 4. 实现自定义聚合函数显示备注
 */
object PopularProductsTopN {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PopularProductsTopN")
      .config("spark.master", "local[1]")
      .getOrCreate()

    val basePath = "./src/main/resources/spark_sql/"

    val userDataSet = userToDataSet(basePath + "user_visit_action.txt", spark)
    val cityDataSet = cityToDataSet(basePath + "city_info.txt", spark)
    val productDataset = productToDataSet(basePath + "product_info.txt", spark)

    userDataSet.createOrReplaceTempView("user_visit_action")
    cityDataSet.createOrReplaceTempView("city_info")
    productDataset.createOrReplaceTempView("product_info")

    // 1.先关联三张表求出区域、城市名、商品名称
    spark.sql(
      """
        | select
        |   a.*,
        |   p.product_name,
        |   c.area,
        |   c.city_name
        |  from user_visit_action a
        |  join product_info p on a.click_product_id = p.product_id
        |  join city_info c on a.city_id = c.city_id
        |  where a.click_product_id > -1
        |""".stripMargin
    ).createOrReplaceTempView("t1")

    // 根据区域，商品进行数据聚合
    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF))

    // 2.根据区域和商品分组，统计每个区域的商品点击次数
    spark.sql(
      """
        | select
        |  area,
        |  product_name,
        |  count(*) as clickCnt,
        |  cityRemark(city_name) as city_remark
        | from t1 group by area, product_name
        |""".stripMargin
    ).createOrReplaceTempView("t2")

    /**
    +----+------------+--------+------------------------------+----+
    |东北|商品_41     |169     |哈尔滨 35%, 大连 34%, 其他 31%|1   |
    |东北|商品_91     |165     |哈尔滨 35%, 大连 32%, 其他 33%|2   |
    |东北|商品_58     |159     |沈阳 37%, 大连 32%, 其他 31%  |3   |
    |东北|商品_93     |159     |哈尔滨 38%, 大连 37%, 其他 25%|3   |
     */

    // 3.统计出每个区域的热门商品，取前三
    spark.sql(
      """
        | select *,
        |   rank() over (partition by area order by clickCnt desc) as rank
        | from t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 取前3名
    spark.sql(
      """
        | select
        |     *
        | from t3 where rank <= 3
      """.stripMargin).show(false)

    spark.close()
  }

  /**
   * 用户访问记录 => 读取 txt文件并转换成为 DataSet
   * @param path
   * @param spark
   * @return
   */
  def userToDataSet(path: String, spark: SparkSession): Dataset[UserVisitAction] = {
    import spark.implicits._
    val frame: DataFrame = spark.read
      .schema(Encoders.product[UserVisitAction].schema)
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(path)
      .toDF("date","user_id","session_id","page_id","action_time","search_keyword","click_category_id",
        "click_product_id","order_category_ids","order_product_ids","pay_category_ids","pay_product_ids","city_id")

    frame.as[UserVisitAction]
  }

  /**
   * 城市信息 => 读取txt文件并转换成为DataSet
   * @param path
   * @param spark
   * @return
   */
  def cityToDataSet(path: String, spark: SparkSession): Dataset[CityInfo] = {
    // 读取外部 txt 文件
    val cityRDD: RDD[String] = spark.sparkContext.textFile(path)
    // 导入隐式转换，否则 RDD 不能够调用 toDS 方法
    import spark.implicits._

    cityRDD.map(line => {
      val dataArray = line.split("\\s+")
      CityInfo(dataArray(0).toLong, dataArray(1), dataArray(2))
    }).toDS()
  }


  def productToDataSet(path: String, spark: SparkSession): Dataset[ProductInfo] = {
    val productRDD: RDD[String] = spark.sparkContext.textFile(path)
    // 导入隐式转换
    import spark.implicits._

    productRDD.map(line => {
      val productArray = line.split("\\s+")
      ProductInfo(productArray(0).toLong, productArray(1), productArray(2))
    }).toDS()
  }

  abstract class Base extends Serializable with Product
  // 用户访问动作表(user_visit_action.txt)
  case class UserVisitAction(
    date: String,//用户点击行为的日期
    user_id: Option[Long],//用户的ID
    session_id: String,//Session的ID
    page_id: Option[Long],//某个页面的ID
    action_time: String,//动作的时间点
    search_keyword: String,//用户搜索的关键词
    click_category_id: Option[Long],//某一个商品品类的ID
    click_product_id: Option[Long],//某一个商品的ID
    order_category_ids: String,//一次订单中所有品类的ID集合
    order_product_ids: String,//一次订单中所有商品的ID集合
    pay_category_ids: String,//一次支付中所有品类的ID集合
    pay_product_ids: String,//一次支付中所有商品的ID集合
    city_id: Option[Long] //城市ID
  ) extends Base

  // 商品信息表
  case class ProductInfo(
    product_id: Long, //商品ID
    product_name: String, //商品名称
    extend_info: String //商品平台类型
  ) extends Base

  // 城市信息表
  case class CityInfo(
   city_id: Long, //城市ID
   city_name: String, //城市名称
   area: String //城市所属区域
  ) extends Base


  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {
    // 缓冲区初始化
    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    // 更新缓冲区数据
    override def reduce(buffer: Buffer, city: String): Buffer = {
      buffer.total += 1
      val newCount = buffer.cityMap.getOrElse(city, 0L) + 1
      buffer.cityMap.update(city, newCount)
      buffer
    }

    // 合并缓冲区数据
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total

      val map1 = b1.cityMap
      val map2 = b2.cityMap

      // 将两个map合并的操作
//      b1.cityMap = map1.foldLeft(map2) {
//        case(map, (city, cnt)) => {
//          val newCount = map.getOrElse(city, 0L) + cnt
//          map.update(city, newCount)
//          map
//        }
//      }

      map2.foreach {
        case (city, cnt) => {
          val newCount = map1.getOrElse(city, 0L) + cnt
          map1.update(city, newCount)
        }
      }

      b1.cityMap = map1
      b1
    }

    // 将统计的结果生成字符串信息
    override def finish(reduction: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalCnt = reduction.total
      val cityMap = reduction.cityMap

      // 降序排列
      val cityCntList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)
//      println(cityCntList.toString()) // List((成都,55), (贵阳,53))

      val hashMore = cityMap.size > 2
      var resSum = 0.0 // 将整数修改为小数，数值精度更好一点
      cityCntList.foreach {
        case (city, cnt) => {
          val res = cnt * 100.0 / totalCnt
          remarkList.append(s"${city} ${res.formatted("%.2f")}%")
          resSum += res
        }
      }

      if (hashMore) {
        remarkList.append(s"其他 ${(100 - resSum).formatted("%.2f")}%")
      }
      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
