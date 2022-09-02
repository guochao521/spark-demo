package geektime.optimize

import org.apache.spark._
import org.apache.spark.storage.StorageLevel

object OptimizeCheckPoint {


  def main(args: Array[String]): Unit = {
    val checkpointDir ="./src/main/resources/checkpoint"
    val conf = new SparkConf().setAppName("SparkRDDTRF").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(checkpointDir)
    val rdd = sc.makeRDD(1 to 200, numSlices = 1)
    rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd.checkpoint()
    rdd.foreach(x=>println(x))

  }

}
