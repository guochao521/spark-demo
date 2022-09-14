package customize.catalyst.priority

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DataTypes.{IntegerType, LongType}

/**
 * @author wangguochao
 * @date 2022/9/14
 */


/**
 * 解决当字段为字符串，但数据为数字类型时，sql中若使用比较符号时，存在类型提升不足的问题。
 * 例如：字段f1为浮点型数据，但是条件给定为：f1>5，spark默认会解析为：cast(cast(f1 as string) as int) > 3
 * 这样就会损失精度，f1=3.3将不会满足条件，被过滤掉
 */
object CastTypeOptimization extends Rule[LogicalPlan] with Logging {

  def checkRule(left: Expression, right: Expression): Boolean = {
    var flag = false
    if ((left.isInstanceOf[Cast] && left.dataType == LongType && right.isInstanceOf[Literal] && right.dataType == LongType)
      || (left.isInstanceOf[Cast] && left.dataType == IntegerType && right.isInstanceOf[Literal] && right.dataType == IntegerType)
    ) {
        flag = true
      }
    flag
  }

  /**
   * 修改强制转换的类型为 DoubleType
   * @param plan 执行计划
   * @return 新的执行计划
   */
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformAllExpressions {
      case GreaterThan(left, right) if (checkRule(left, right)) => {
        val leftCastExp = Cast(left.children.head, DataTypes.DoubleType)
        GreaterThan(leftCastExp, right)
      }

      case LessThan(left, right) if (checkRule(left, right)) => {
        val leftCastExp = Cast(left.children.head, DataTypes.DoubleType)
        LessThan(leftCastExp, right)
      }

      case GreaterThanOrEqual(left, right) if(checkRule(left, right)) => {
        val leftCastExp = Cast(left.children.head, DataTypes.DoubleType)
        GreaterThanOrEqual(leftCastExp, right)
      }

      case LessThanOrEqual(left, right) if(checkRule(left, right)) => {
        val leftCastExp = Cast(left.children.head, DataTypes.DoubleType)
        LessThanOrEqual(leftCastExp, right)
      }
    }
  }
}
