package geektime.graphx

import org.apache.spark._
import org.apache.spark.graphx.{VertexId, _}
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

//https://waltyou.github.io/Spark-GraphX/
//https://aiyanbo.gitbooks.io/spark-programming-guide-zh-cn/content/graphx-programming-guide/graph-algorithms.html
object GraphXDemo {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("GraphXDemo").setMaster("local")
    val sc = new SparkContext(conf)
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    graph.vertices.filter { case (id, (name, pos)) => pos == "" }.foreach(x => println(x))
    graph.edges.filter(e => e.dstId > e.srcId).foreach(x => println(x))

    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))

  }
}
