package geektime.ml

import org.apache.spark.{SparkConf, SparkContext}

object DimensionalityReductionExample {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.SingularValueDecomposition
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
    val conf = new SparkConf().setAppName("DimensionalityReductionExample").setMaster("local")
    val sc = new SparkContext(conf)
    val rows = sc.parallelize(data)
    val mat: RowMatrix = new RowMatrix(rows)
    // 计算前5个奇异值和相应的奇异向量。
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
    val U: RowMatrix = svd.U  // U 因子是一个行矩阵RowMatrix.
    val s: Vector = svd.s     // 奇异值存储在本地向量中
    val V: Matrix = svd.V     // V 因子

  }
}
