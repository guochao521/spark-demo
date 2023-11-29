package customize.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * @author wangguochao
 * @date 2023/4/5 20:48
 *
 * 链接：https://www.lilinchao.com/archives/1622.html
 */
object UserContinueLoginRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("UserContinueLoginRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // 读取数据
    val rdd: RDD[String] = sc.textFile("./src/main/resources/spark_sql/v_user_login.csv")

    // 过滤第一行表头
    val header = rdd.first()
    val rddData = rdd.filter(row => row != header)
    // 对数据进行处理
    val uidAndDate: RDD[(String, String)] = rddData.map(x => {
      val fis = x.split(",")
      val uid = fis(0)
      val date = fis(1).replace("/", "-")
      (uid, date)
    })

    // 根据uid 进行分组，将同一个用户的登录数据搞到同一个分组中
    val grouped: RDD[(String, Iterable[String])] = uidAndDate.groupByKey()

    // 定义一个日期的工具类
    val calendar = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    // 在组内进行排序
    val uidAndDateDif = grouped.flatMapValues(it => {
      // 将迭代器中的数据toList/toSet，有可能会发生内存溢出
      val sorted = it.toSet.toList.sorted
      var index = 0
      sorted.map(desStr => {
        val date = sdf.parse(desStr)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -index)
        index += 1
        (desStr, sdf.format(calendar.getTime))
      })
    })

    val sortedBuffer = uidAndDateDif.collect().toBuffer
    println(sortedBuffer)


    val result = uidAndDateDif.map(x => {
      ((x._1, x._2._2), x._2._1)
    }).groupByKey().mapValues(it => {
      val list = it.toList
      val times = list.size
      val startTime = list.head
      val endTime = list.last
      (times, startTime, endTime)
    }).map(t => {
      (t._1._1, t._2._1, t._2._2, t._2._3)
    }).filter(x => {
      x._2 >= 3
    })


    val buffer = result.collect().toBuffer
    println(buffer)
  }

}
