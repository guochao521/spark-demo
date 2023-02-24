package customize.deltalake

import io.delta.sql.parser.DeltaSqlParser
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.delta.{DeltaAnalysis, DeltaUnsupportedOperationsCheck, OptimisticTransaction, PreprocessTableDelete, PreprocessTableMerge, PreprocessTableUpdate, Snapshot}

/**
 * @author wangguochao
 * @date 2023/2/24 15:46
 */

/**
 * 这个例子：
 * DeltaLake 扩展了 Spark 的功能，注入了很多规则，它里面提供了一些接口，
 * 这些 SQL 帮助我们去创建一些 Delta 表；
 * Delta Lake里面，其实复用了 Spark 的所有的物理执行计划。
 */
class DeltaSparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new DeltaSqlParser(parser)
    }
    extensions.injectResolutionRule { session =>
      new DeltaAnalysis(session)
    }
    extensions.injectCheckRule { session =>
      new DeltaUnsupportedOperationsCheck(session)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableUpdate(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableMerge(session.sessionState.conf)
    }
    extensions.injectPostHocResolutionRule { session =>
      new PreprocessTableDelete(session.sessionState.conf)
    }
//    extensions.injectOptimizerRule { session =>
//      new ActiveOptimisticTransactionRule(session)
//    }
  }
}
