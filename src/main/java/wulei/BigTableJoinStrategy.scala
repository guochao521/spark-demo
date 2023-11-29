//package wulei
//
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.lit
//
///**
// * @author wangguochao
// * @date 2023/5/4 21:22
// */
//object BigTableJoinStrategy {
//
//  def main(args: Array[String]): Unit = {
//
//
//
//    //统计订单交易额的代码实现
//    val txFile: String = _
//    val orderFile: String = _
//
//    val transactions: DataFrame = spark.read.parquent(txFile)
//    val orders: DataFrame = spark.read.parquent(orderFile)
//
//    transactions.createOrReplaceTempView(“transactions”)
//    orders.createOrReplaceTempView(“orders”)
//
//    val query: String =
//      """
//        |    select sum(tx.price * tx.quantity) as revenue, o.orderId
//        |    from transactions as tx inner join orders as o
//        |    on tx.orderId = o.orderId
//        |    where o.status = ‘COMPLETE’
//        |    and o.date between ‘2020-01-01’ and ‘2020-03-31’
//        |    group by o.orderId
//        |""".stripMargin
//
//
//    val outFile: String = _
//    spark.sql(query).save.parquet(outFile)
//
//
//
//    //根据Join Keys是否倾斜、将内外表分别拆分为两部分
//    import org.apache.spark.sql.functions.array_contains
//
//    //将Join Keys分为两组，存在倾斜的、和分布均匀的
//    val skewOrderIds: Array[Int] = _
//    val evenOrderIds: Array[Int] = _
//
//    val skewTx: DataFrame = transactions.filter(array_contains(lit(skewOrderIds), $"orderId"))
//    val evenTx: DataFrame = transactions.filter(array_contains(lit(evenOrderIds),$"orderId"))
//
//    val skewOrders: DataFrame = orders.filter(array_contains(lit(skewOrderIds),$"orderId"))
//    val evenOrders: DataFrame = orders.filter(array_contains(lit(evenOrderIds),$"orderId"))
//
//
//    val skewQuery: String = "" +
//      "select /*+ shuffle_hash(orders) */ sum(tx.price * tx.quantity) as initialRevenue, o.orderId, o.joinKey" +
//      "from saltedSkewTx as tx inner join saltedskewOrders as o" +
//      "on tx.joinKey = o.joinKey" +
//      "where o.status = ‘COMPLETE’" +
//      "and o.date between ‘2020-01-01’ and ‘2020-03-31’" +
//      "group by o.joinKey"
//
//
//
//  }
//
//}
