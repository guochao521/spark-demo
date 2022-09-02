//package SparkES
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.elasticsearch.spark.rdd.EsSpark
//
//
///**
//  *  cd ~/Documents/alex/dev/evn/elasticsearch-7.4.2 && bin/elasticsearch
//  */
//object SparkES {
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setAppName("SparkES").setMaster("local")
//     .set("cluster.name", "es")
//     .set("es.index.auto.create", "true")
//     .set("es.nodes", "127.0.0.1")
//     .set("es.port", "9200")
//     .set("es.index.read.missing.as.empty","true")
////     .set("es.net.http.auth.user", "elastic") //访问es的用户名
////     .set("es.net.http.auth.pass", "changeme") //访问es的密码
//     .set("es.nodes.wan.only","true")
//    val sc = new SparkContext(sparkConf)
//    // DataFrame schema automatically inferred
//    //使用elasticsearch-spark-20_2.11
//     // writeToEs(sc) //写
//      readFromEs(sc); //读
//  }
//
//    def writeToEs(sc: SparkContext) = {
//      val user1 = Map("name" -> "alex", "age" -> 30, "city" -> "beijing")
//      val user2 = Map("name" -> "vic", "age" -> 31, "city" -> "xian")
//      var rdd = sc.makeRDD(Seq(user1, user2))
//      println("***********prepare save**************")
//      EsSpark.saveToEs(rdd, "user/manager") //index/type
//      println("***********end save**************")
//    }
//
//      def readFromEs(sc: SparkContext) {
//        val query = "{\"query\":{\"bool\":{\"must\":[{\"match_all\":{}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[],\"aggs\":{}}";
//        val rdd = EsSpark.esRDD(sc, "user/manager",query)
//        val basePath = "./src/main/resources/"
//        println("rdd.count():" + rdd.count())
//        //rdd.saveAsTextFile(basePath+"es_data.txt")
//        rdd.foreach(line => {
//          val key = line._1
//          val value = line._2
//          println(line)
//          for (tmp <- value) {
//            val key1 = tmp._1
//            val value1 = tmp._2
//           // println(tmp)
//          }
//        })
//      }
//
//}
