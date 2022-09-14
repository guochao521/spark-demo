package customize.catalyst

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}
import org.apache.spark.sql.SparkSession


/**
 * @author wangguochao
 * @date 2022/9/14
 */
case class SqlEvent(sqlText: String, sparkSession: SparkSession) extends org.apache.spark.scheduler.SparkListenerEvent with Logging

class MySparkAppListener(sqlText: String, sparkSession: SparkSession) extends SparkListener with Logging {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId
    logInfo("***************************************************" + appId.get)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logInfo("************************ app end time ************************ " + applicationEnd.time)
  }
  SqlEvent(sqlText, sparkSession)
}