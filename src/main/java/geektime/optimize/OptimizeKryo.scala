package geektime.optimize

import com.esotericsoftware.kryo.Kryo
import org.apache.spark._
import org.apache.spark.serializer.KryoRegistrator

object OptimizeKryo {

  //两个成员变量name和age，同时必须实现java.io.Serializable接口
  class Persion1(val name: String, val age: Int) extends java.io.Serializable {

    override def toString = s"Persion1($name, $age)"
  }

  //两个成员变量name和city，同时必须实现java.io.Serializable接口
  class Persion2(val name: String, val city: String) extends java.io.Serializable {

    override def toString = s"Persion2($name, $city)"
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OptimizeKryo").setMaster("local")
    //1：数据本地性调优
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "geekbang.optimize.MyKryoRegistrator")

    val sc = new SparkContext(conf)
    val rdd_kryo = sc.parallelize(List(new Persion1("Tom", 31), new Persion2("Jack", "Beijing")))
    rdd_kryo.foreach(x => println(x.toString))

  }

}
