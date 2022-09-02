package geektime.streaming

import geektime.constant.Constant
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileStreaming {

  import org.apache.spark.SparkConf

    def main(args: Array[String]): Unit = {
      try {

      //1：创建一个本地SparkConf
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        //2：创建一个StreamingContext，每1s处理一次数据
        val ssc = new StreamingContext(conf, Seconds(10))
        //3：创建一个DStream并监听文件流
         val lines = ssc.textFileStream(Constant.basePath + "structured_dir")
        // 5：打印在DStream中每个RDD的前10行数据
        System.out.println("wordCounts:" + lines)
        lines.print()
        //6：调用start方法启动流计算
        ssc.start()
        //7：等待流计算完成
        ssc.awaitTermination()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }


}
