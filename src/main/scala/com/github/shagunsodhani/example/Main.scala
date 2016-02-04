package main.scala.com.github.shagunsodhani.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import main.scala.com.github.shagunsodhani.utils.SparkContextUtils
import main.scala.com.github.shagunsodhani.cliqueEnum.{BronKerbosch => BronKerbosch}

object Main {
  def main(args: Array[String]) {

    val sc: SparkContext = SparkContextUtils.getSparkContext

    val users: RDD[(Long, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val graph = Graph(users, relationships)
    
    val bk = new BronKerbosch(sc, graph).runAlgorithm;
    
    bk.foreach { println }
  }
}