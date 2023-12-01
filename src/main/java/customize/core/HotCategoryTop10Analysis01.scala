package customize.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/11/30 13:25
 */
object HotCategoryTop10Analysis01 {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)
    sc.setLogLevel("WARN")

    // 1.读取原始日志数据
    val actionRDD = sc.textFile("./src/main/resources/spark_core/user_visit_action.txt")

    // 2.统计品类的点击数量：（品类，点击数量）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3.统计品类的下单数量：（品类，下单数量）
    val orderActionRDD = actionRDD.filter(
      actions => {
        val datas = actions.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4.统计品类的支付数量：（品类id, 支付数量）
    val playActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val playCountRDD = playActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id =>(id,1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类进行排序，并且取前10名
    // 点击数量排序，下单数量排序，支付数量排序
    // 元祖排序：先比较第一个，再比较第二个，再比较第三个，依次类推
    // ( 品类ID, ( 点击数量, 下单数量, 支付数量 ) )
    val rdd1 =clickCountRDD.map({
      case (cid, cnt) => (cid, (cnt, 0, 0))
    })

    val rdd2 = orderCountRDD.map({
      case (cid, cnt) => (cid, (0, cnt, 0))
    })

    val rdd3 = playCountRDD.map({
      case (cid, cnt) => (cid, (0, 0, cnt))
    })

    // union: 并集
    val sourceRDD = rdd1.union(rdd2).union(rdd3)
    sourceRDD.foreach(println)

    val analysisRDD = sourceRDD.reduceByKey(
      (t1,t2) => {
        (t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val resultRDD = analysisRDD.sortBy(_._2, ascending = false).take(10)

    //6. 将结果采集到控台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }

}
