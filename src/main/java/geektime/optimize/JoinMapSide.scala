package geektime.optimize

import org.apache.spark.{SparkConf, SparkContext}

object JoinMapSide {
  def main(args: Array[String]): Unit = {

    //1:初始化SparkContext
    val conf = new SparkConf().setAppName("JoinMapSide").setMaster("local")
    val sc = new SparkContext(conf)
    val map = Map("alex"-> "shanghai", "Andy"-> "beijing" )
    val b1= sc.broadcast(map)
    val rdd_2 = sc.makeRDD( Seq(
      ("alex", "20"),
      ("Andy", "30" )))

    rdd_2.map(data=>{
        val key = data._1;
        val value = data._2;
        val map =  b1.value
        (key,("city"->map.get(key),"age"->value))
    }).foreach(x=>{
        if(x != ()){
         println(x)
        }
    })


    Thread.sleep(1000000000)

  }
}
