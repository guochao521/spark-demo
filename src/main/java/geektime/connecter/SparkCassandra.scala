package geektime.connecter

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * (1)启动Cassandra
  * cd /Users/wangleigis163.com/Documents/alex/dev/evn/apache-cassandra-3.11.4/bin && ./cassandra
  * （2） 登录Cassandra，查看集群信息【 SELECT cluster_name, listen_address FROM system.local;】。
  * ./cqlsh
  * （3）创建Keyspace和表。
  * create KEYSPACE sparkdb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  * create table sparkdb.people(id text, name text,   age int,job text, primary key (id));
  * select * from sparkdb.people;
  */
object SparkCassandra {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkCassandra")
      .getOrCreate()


    val basePath = "./src/main/resources/"


    val peopleDF = spark.read.format("csv").option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true") //将第一行进行检查并推断其scheme
      .load(basePath + "people-cassandra.csv")

    peopleDF.show()


    peopleDF.write
      .format("org.apache.spark.sql.cassandra")
      .mode("overwrite")
      .option("confirm.truncate", "true")
      .option("spark.cassandra.connection.host", "127.0.0.1")
      .option("spark.cassandra.connection.port", "9042")
      .option("keyspace", "sparkdb")
      .option("table", "people")
      .save()


    val df_read = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", "127.0.0.1")
      .option("spark.cassandra.connection.port", "9042")
      .option("keyspace", "sparkdb")
      .option("table", "people")
      .load()
    df_read.show

    df_read.createTempView("view_cassadra_people")

    spark.sql("select * from view_cassadra_people where id>1").show()

  }

}
