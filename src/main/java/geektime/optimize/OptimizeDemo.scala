package geektime.optimize

import com.esotericsoftware.kryo.Kryo
import org.apache.spark._
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.storage.StorageLevel

object OptimizeDemo {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRDDTRF").setMaster("local")

    //1：数据本地性调优
    conf.set("spark.locality.wait","3s")
    conf.set("spark.locality.wait.process","15s")
    conf.set("spark.locality.wait.node","30s")
    conf.set("spark.locality.wait.rack","45s")

    val sc = new SparkContext(conf)
    // var list = List("apple", "bag bag cat", "cat cat");
    val basePath = "./src/main/resources/"
    val rdd  = sc.textFile(basePath+"kv1.txt",2)
    rdd.mapPartitions( iteratorData => {
      println("partitionId:"+TaskContext.getPartitionId())
      //分区内的数据解析
      val bathData = iteratorData.toList
      println("begin process for bath,length:"+bathData.size)
      //这里实现分区内的数据批量处理逻辑,比如数据的批量写入
      println("end process for bath,finished:"+bathData.size)
      Iterator(bathData.size)
    }).collect()


    rdd.mapPartitionsWithIndex( (partitionIndex,iteratorData) => {
      println("partitionId:"+partitionIndex)
      //分区内的数据解析
      val bathData = iteratorData.toList
      println("begin process for bath,length:"+bathData.size)
      //这里实现分区内的数据批量处理逻辑,比如数据的批量写入
      println("end process for bath,finished:"+bathData.size)
      Iterator(bathData.size)
    }).collect()

    rdd.foreachPartition(partitionDatas=>
     {
       val bathData = partitionDatas.toList
       println(" foreachPartition begin  process for bath,length:"+bathData.size)
       //这里实现分区内的数据批量处理逻辑,比如数据的批量写入
        println(" foreachPartition end process for bath,finished:"+bathData.size)
     })

    rdd.repartition(3)
    rdd.coalesce(100,true)


    rdd.map(x=>(x,1)).repartition(2).sortByKey(true)
    rdd.map(x=>(x,1)).repartitionAndSortWithinPartitions(new HashPartitioner(2))
    rdd.map(x=>(x,1)).reduceByKey((x,y)=>(x+y)).collect().foreach(x=>print(x))
    rdd.map(x=>(x,1)).groupByKey().collect().foreach(x=>print(x))

    rdd.cache()

//    rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)


  }

}
