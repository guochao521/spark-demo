package geektime.connecter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  *   cd ~/Documents/alex/dev/evn/hbase-1.4.10
  *   bin/start-hbase.sh
  *   bin/hbase shell
  *  truncate 'person'
  *  scan 'person'
  */
object SparkHBase {

  val HBASE_ZOOKEEPER_QUORUM = "localhost"
  val tableName = "person"
  //   主函数
  def main(args: Array[String]) {

    // 设置spark访问入口
    val conf = new SparkConf().setAppName("SparkHBase ")
    .setMaster("local")//调试
    val sc = new SparkContext(conf)

    val indataRDD: RDD[String] = sc.makeRDD(Array("1,jack,15","2,Lily,16","3,mike,16"))


    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的
    val jobConf = new JobConf(getHbaseConf())
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val rdd = indataRDD.map(_.split(',')).map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
      (new ImmutableBytesWritable, put)
    }}
    rdd.saveAsHadoopDataset(jobConf)


    // 获取HbaseRDD
    val hbaseRDD = sc.newAPIHadoopRDD(getHbaseConf(), classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      hbaseRDD.map(_._2).map(result2Map(_)).foreach( x=>println(x.toList))
  }


  def result2Map(result: org.apache.hadoop.hbase.client.Result): Map[String,String] = {
    val rowkey =Bytes.toString(result.getRow())
    val name = Bytes.toString(result.getValue("info".getBytes,"name".getBytes))
    val age = Bytes.toString(result.getValue("info".getBytes,"age".getBytes))
    Map("rowkey"->rowkey,"name" ->name, "age" -> age)//scala最后一行为返回
  }

  // 构造 Hbase 配置信息
  def getHbaseConf(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    // 设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf
  }

}
