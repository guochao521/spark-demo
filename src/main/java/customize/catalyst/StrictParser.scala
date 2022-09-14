package customize.catalyst

import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * @author wangguochao
 * @date 2022/8/10
 */
class StrictParser(parser: ParserInterface) extends ParserInterface {

  /**
   * 解析一个字符串到 [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val logicalPlan: LogicalPlan = parser.parsePlan(sqlText)
    logicalPlan transform {
      // 这个 @ 的主要功能是在模式匹配中，匹配到一个模式后，但是在处理过程中，使用对象本身而不是匹配后的元素。
      // @ 模式匹配，加上 @ 匹配的是 Project(projectList, _)
      case project @ Project(projectList, _) =>
        projectList.foreach {
          name =>
            if (name.isInstanceOf[UnresolvedStar]) {
              throw new RuntimeException("You must specify your project column set," +
                " * is not allowed.")
            }
        }
        project
    }
    logicalPlan
  }

  /**
   * Parse a string to an [[Expression]].
   */
  override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

  /**
   * Parse a sting to a [[TableIdentifier]].
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parser.parseTableIdentifier(sqlText)

  /**
   * Parse a sting to a [[FunctionIdentifier]].
   * @param sqlText
   * @return
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = parser.parseFunctionIdentifier(sqlText)

  /**
   * Parse a string to a [[Seq]]
   * @param sqlText
   * @return
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = parser.parseMultipartIdentifier(sqlText)

  /**
   * Parse a string to a [[StructType]]. The passed SQL string should be a comma separated list of
   * field definitions which will preserved the correct Hive metadata.
   * @param sqlText
   * @return
   */
  override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)

  /**
   * Parse a string to a [[DataType]]
   * @param sqlText
   * @return
   */
  override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)
}