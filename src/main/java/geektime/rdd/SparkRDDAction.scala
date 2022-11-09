package geektime.rdd

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * 15讲代码
  */
object SparkRDDAction {
  def main(args: Array[String]): Unit = {
    try {
      //1:初始化SparkContext
      val conf = new SparkConf().setAppName("SparkRDDTRF").setMaster("local")
      val sc = new SparkContext(conf)

      sc.setLogLevel("WARN")
//      conf.set("spark.scheduler.mode", "FAIR")
//      sc.setLocalProperty("spark.scheduler.pool", "testPoll")

      //      var list = List("apple", "bag bag cat", "cat cat");
      val basePath = "./src/main/resources/"
      //2:初始数据到RDD,参数为2个分区
      val rdd = sc.textFile(basePath + "simple", 2)
      rdd.foreachPartition(t => {
        val id =  TaskContext.get.partitionId
        println("partitionNum:" + id)
        t.foreach( data => {
          println(data)
        })
      })

      rdd.flatMap(x=> {x.split(" ")}).map(x => (x,1)).foreach( x => println(x))

      rdd.flatMap(x=> {x.split(" ")})
        .filter(x => x!="bag")
        .foreach(x => println(x))


      rdd.flatMap(x=> {x.split(" ")})
        .collect().foreach(x => println(x))

      rdd.flatMap(x=> {x.split(" ")})
        .mapPartitions( x => {
          println(TaskContext.getPartitionId())
          Iterator(x.length)})
        .collect()
        .foreach(x => println(x))

      rdd.flatMap(x=> {x.split(" ")})
        .mapPartitionsWithIndex( (index,data) => {
          println("index:"+index)
          println("data:"+data)
          Iterator(index)
          }
        ).foreach(x=>println("index:"+x))
      println("union")
      rdd.union(rdd).foreach(x => println(x))

      rdd.flatMap(x=> {x.split(" ")}).distinct().foreach(x => println(x))


      rdd.flatMap(x=> {x.split(" ")}).map(x => (x,1)).groupByKey().foreach(x => println(x))

      val map=  rdd.flatMap(x=> {x.split(" ")}).map(x => (x,1))
      map.reduceByKey((pre, after) => pre + after)
        .foreach(x => println(x))

      map.reduceByKey((pre, after) => pre + after).sortByKey().collect.foreach(x => println(x))

      var reduceByKeyResult = map.reduceByKey((pre, after) => pre + after)
      reduceByKeyResult.join(reduceByKeyResult).foreach(x => println(x))
//      rdd.repartition(1)
//        .foreachPartition(t => {
//          val id =  TaskContext.get.partitionId
//          println("partitionNum:" + id)
//          t.foreach( data => {
//            println(data)
//          })
//        })

      rdd.flatMap(x=> {x.split(" ")}).sample(false,0.5,5).foreach(x=>println(x))


      reduceByKeyResult.cogroup(reduceByKeyResult).collect().foreach(x => println(x))

      reduceByKeyResult.cartesian(reduceByKeyResult).repartition(4).foreachPartition(t => {
        val id =  TaskContext.get.partitionId
        println(" cartesian partitionNum:" + id)
        t.foreach( data => {
          println(data)
        })
      })

      println(
        rdd.reduce(
          (x, y) => x + " " + y))


      import org.apache.spark.broadcast.Broadcast
      val broadcastVar: Broadcast[Array[Int]] = sc.broadcast(Array[Int](1, 2, 3))
      rdd.foreach(x=>{
        val  bc = broadcastVar.value
        val result = bc.mkString("-")+"-"+x
        println(result)
      })
      broadcastVar.unpersist()
      broadcastVar.destroy()

//      rdd.map(x=>{
//        val  bc = broadcastVar.value
//        val result = bc.mkString("-")+"-"+x
//        (x,result)
//      }).take(3)
      broadcastVar.unpersist()
      broadcastVar.destroy()

      /*rdd.foreach(x=>println(x))
      println(rdd.reduce((x, y) => x + " " + y))
      rdd.collect().foreach(x =>println(x))
      println("***********countByKey*************")
      rdd.flatMap(x=> {x.split(" ")}).map(x => (x,1)).countByKey().foreach(x =>println(x))
      rdd.foreach(x => println(x))
      println(rdd.first())
      println(rdd.take(2).toList)
      println(rdd.takeOrdered(2).toList)
     // rdd.saveAsTextFile(basePath+"simple_res")
      rdd.saveAsObjectFile(basePath+"simple_object")*/
      //5：停止SparkContext
      sc.stop()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
