package customize.core.utils

import scala.io.Source

/**
 * @author wangguochao
 * @date 2023/12/1 21:16
 */
object MyUtils {

  def readRules(path: String): Array[(Long, Long, String, String)] = {
    //读取文件
    val source = Source.fromFile(path, "UTF-8")
    val lines: Array[String] = source.getLines().toArray
    //将每一行数据按照 | 分割开
    val rules: Array[(Long, Long, String, String)] = lines.map(
      line => {
        val splited: Array[String] = line.split("\\|")
        //将每一行数据按照 | 分割开后，将每一条数据转换为 Long 类型的 ip 范围
        (ip2Long(splited(0)), ip2Long(splited(1)), splited(6), splited(7))
      }
    ).sortBy(_._1)
    source.close()
    rules
  }

  /**
   * 将 String 类型的 ip 转为 Long 类型的 十进制ip
   * @param ip
   * @return
   */
  def ip2Long(ip: String): Long = {

    //将数据按照 . 分割开
    //192.168.5.1
    val splited: Array[String] = ip.split("[.]")

    //遍历取出的每一条ip，按照特定算法算出 十进制Long 的值
    var ipNum = 0L
    for (i <- splited.indices) {
      ipNum = splited(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
   * 使用二分查找，查询输入的 ip 所对应的地址，并返回该地址对应的Array的index
   * @param ip
   * @return
   */
  def binarySearch(ipArray: Array[(Long, Long, String, String)], ip: Long): Int = {
    //定义一个开始下标
    var startIndex = 0
    //定义一个结束下标
    var endIndex = ipArray.length -1
    //二分查找
    while(startIndex <= endIndex) {
      var midIndex = (startIndex + endIndex) / 2
      if (ip >= ipArray(midIndex)._1 && ip <= ipArray(midIndex)._2) {
        return midIndex   // 此处一定要写 return，这样才会结束该方法，返回midIndex的值
      } else if (ip < ipArray(midIndex)._1) {
        endIndex = midIndex - 1
      } else {
        startIndex = midIndex + 1
      }
    }
    -1  //没有找到就返回 -1
  }
}
