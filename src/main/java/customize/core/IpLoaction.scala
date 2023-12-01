package customize.core

import customize.core.utils.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author wangguochao
 * @date 2023/12/1 20:51
 * @version 1.0.0
 * 参考链接:
 *  1.https://blog.csdn.net/weixin_42419342/article/details/108865070
 *  2.https://lilinchao.com/archives/1607.html
 */
object IpLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // 获取全部IP规则数据
    val rules: Array[(Long, Long, String, String)] = MyUtils.readRules("./src/main/resources/spark_core/ip.txt")

    //调用sc上的广播方法
    val broadcastRef: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(rules)

    //创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile("./src/main/resources/spark_core/access.log")

    val func = (line: String) => {
      //对数据进行切割
      val fields = line.split("[|]")
      //获取IP
      val ip = fields(1)
      //将IP转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      //获取广播方法中IP规则
      val rulesInExecutor: Array[(Long, Long, String, String)] = broadcastRef.value
      //查找
      var province = "未知"
      var city = "null"
      //在IP规则中进行二分查找（IP规则事先是排好序的）
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
        city = rulesInExecutor(index)._4
      }
      ((province, city), 1)
    }

    //整理数据
    val provinceAndOne: RDD[((String, String), Int)] = accessLines.map(func)

    //聚合
    //val sum = (x: Int, y: Int) => x + y
    val reduced: RDD[((String, String), Int)] = provinceAndOne.reduceByKey(_ + _)

    //将结果打印
    println(reduced.collect().toBuffer)
    // ArrayBuffer((陕西,1824), (河北,383), (云南,126), (重庆,868), (北京,1535)) 只有省份
    // ArrayBuffer(((云南,昆明),126), ((北京,北京),1535), ((河北,石家庄),383), ((陕西,西安),1824), ((重庆,重庆),868))

    sc.stop()
  }
}
