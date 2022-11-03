package customize.catalyst

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}
/**
 * @author wangguochao
 * @date 2022/9/14
 */


//case class SqlEvent(sqlText: String, sparkSession: SparkSession) extends org.apache.spark.scheduler.SparkListenerEvent with Logging

class MySqlParser(sparkSession: SparkSession, val delegate : org.apache.spark.sql.catalyst.parser.ParserInterface) extends scala.AnyRef with org.apache.spark.sql.catalyst.parser.ParserInterface with Logging{
  override def parsePlan(sqlText: String): LogicalPlan = {
    logInfo("start to send SqlEvent by listenerBus,sqlText:"+sqlText)
    sparkSession.sparkContext.addSparkListener(new MySparkAppListener(sqlText, sparkSession))
    logInfo("send SqlEvent success by listenerBus,sqlText:"+sqlText)
    delegate.parsePlan(sqlText)
  }

  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }
}