package customize.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/11/30 20:05
 * 页面单跳转换率统计
 * https://lilinchao.com/archives/1600.html
 */
object PageFlowAnalysis {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("PageFlowAnalysis")
    val sc = new SparkContext(sparConf)
    sc.setLogLevel("WARN")
    val actionRDD = sc.textFile("./src/main/resources/spark_core/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val dataList = action.split("_")
        UserVisitAction(
          dataList(0),
          dataList(1).toLong,
          dataList(2),
          dataList(3).toLong,
          dataList(4),
          dataList(5),
          dataList(6).toLong,
          dataList(7).toLong,
          dataList(8),
          dataList(9),
          dataList(10),
          dataList(11),
          dataList(12).toLong
        )
      }
    )
    actionDataRDD.cache()

    // TODO 对指定的页面连接跳转进行统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    // tail:获取除了第一个元素之外的所有元素
    // zip:拉链操作，用于关联两个集合。如果其中一个参数元素比较长，那么多余的参数会被删掉。
    val okFlowIds = ids.zip(ids.tail)
//    okFlowIds.foreach(println)

    // 筛选过滤，对指定页面，根据页面id进行聚合操作
    val pageIdToCountMap = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => (action.page_id, 1L)
    ).reduceByKey(_ + _).collect().toMap
//    pageIdToCountMap.foreach(println)
    // TODO 计算分子

    // 根据session进行分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)
    // 分组后，根据访问时间进行排序
    val mvRDD = sessionRDD.mapValues(
      iter => {
        val sortList = iter.toList.sortBy(_.action_time)
        // 【1，2，3，4】
        // 【1，2】，【2，3】，【3，4】
        // 【1-2，2-3，3-4】
        // Sliding : 滑窗
        // 【1，2，3，4】
        // 【2，3，4】
        // zip : 拉链
        val flowIds = sortList.map(_.page_id)
        val pageFlowIds = flowIds.zip(flowIds.tail)

        // 将不合法的页面跳转进行过滤
        pageFlowIds.filter(
          flow => {
            okFlowIds.contains(flow)
          }
        ).map(flow => (flow, 1))
      }
    )


    val flatRDD = mvRDD.map(_._2).flatMap(list => list)
    val dataRDD = flatRDD.reduceByKey(_+_)
//    flatRDD.foreach(println)
    // TODO 计算单跳转换率
    dataRDD.foreach({
      case ((pageId1, pageId2), sum) => {
        val lon = pageIdToCountMap.getOrElse(pageId1, 0L)
        println(s"页面${pageId1}跳转到页面${pageId2}单跳转换率为:" + (sum.toDouble/lon))
      }
    })

    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
    date: String, //用户点击行为的日期
    user_id: Long, //用户的ID
    session_id: String, //Session的ID
    page_id: Long, //某个页面的ID
    action_time: String, //动作的时间点
    search_keyword: String, //用户搜索的关键词
    click_category_id: Long, //某一个商品品类的ID
    click_product_id: Long, //某一个商品的ID
    order_category_ids: String, //一次订单中所有品类的ID集合
    order_product_ids: String, //一次订单中所有商品的ID集合
    pay_category_ids: String, //一次支付中所有品类的ID集合
    pay_product_ids: String, //一次支付中所有商品的ID集合
    city_id: Long //城市 id
  )

}
