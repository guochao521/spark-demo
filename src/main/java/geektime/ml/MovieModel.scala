package geektime.ml

import java.util
import java.util.Arrays

import breeze.stats.hist
import com.github.sh0nk.matplotlib4j.Plot
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spire.math.Number

import com.github.sh0nk.matplotlib4j.builder

import scala.collection.JavaConverters._

/**
  * 数据说明：
  * u.user文件包含user.id、age、gender、occupation和ZIP code这些 属性，各属性之间用管道符(|)分隔
  * u.item文件则包含movie id、title、release date以及若干与IMDB link和电影分类相 关的属性。
  *u.data文件包含user id、movie id、rating(从1到5)和timestamp属性
  */
object MovieModel {
  def main(args: Array[String]): Unit = {
    //1:初始化SparkContext
    val conf = new SparkConf().setAppName("SparkRDDTRF").setMaster("local")
    val sc = new SparkContext(conf)
    //      var list = List("apple", "bag bag cat", "cat cat");
    val basePath = "./src/main/resources/ml/ml-100k/"
    //2:加载用户信息
    val user_data = sc.textFile(basePath + "u.user")
    user_data.take(3)
    val user_fields = user_data.map(x=>x.split('|'))
    val num_users = user_fields.map( fields=> fields.apply(0)).count()

   val num_genders = user_fields.map( fields=> fields.apply(2)).distinct().count()
    println("******************")
    println(num_genders)
     val num_occupations = user_fields.map( fields=> fields.apply(3)).distinct().count()
    println("******************")
    println(num_occupations)
     val num_zipcodes = user_fields.map( fields=> fields.apply(4)).distinct().count()
     println("Users: %d, genders: %d, occupations: %d, ZIP codes: %d".format(num_users, num_genders, num_occupations, num_zipcodes))
    val ages = user_fields.map(x => Integer.parseInt(x.apply(1))).collect().toList








    import com.github.sh0nk.matplotlib4j.Plot
    import com.github.sh0nk.matplotlib4j.builder.HistBuilder
    //用户年龄分布
    val plt: Plot = Plot.create
    val list:java.util.List[Number] =new util.ArrayList[Number]()
    ages.foreach(x=>list.add(x))
    plt.hist.add(list).orientation(HistBuilder.Orientation.vertical)
    //plt.ylim(0, 100)
    plt.title("ages histogram")
    plt.show




  }
}
