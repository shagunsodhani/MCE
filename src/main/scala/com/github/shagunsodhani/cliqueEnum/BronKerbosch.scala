package main.scala.com.github.shagunsodhani.cliqueEnum

import org.slf4j.LoggerFactory
import org.slf4j.Logger
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import scala.collection.mutable.{ Set => MutableSet }
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeDirection

class BronKerbosch[VD: ClassTag, ED: ClassTag](sc: SparkContext,
                                               inputGraph: Graph[VD, ED]) {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BronKerbosch[VD, ED]]);

  private val sparkContext: SparkContext = sc;

  private val graph: Graph[VD, ED] = inputGraph;

  def runAlgorithm = {
    logger.info("Starting BronKerbosch Algorithm");
    val potentialClique = MutableSet[Long]()
    val candidates = graph.vertices
      .map(vertex => vertex._1.asInstanceOf[Long]).collect().toSet;
    candidates.foreach(println)
    val alreadyFound = MutableSet[Long]();
    val cliques = MutableSet[MutableSet[Long]]()
    findCliques(potentialClique, candidates, alreadyFound, cliques);
    cliques;
  }

  private def findCliques(potentialClique: MutableSet[Long],
                          candidates: Set[Long], alreadyFound: MutableSet[Long],
                          cliques: MutableSet[MutableSet[Long]]): Unit = {
    logger.info("Starting Find Clique Method")
    if (candidates.isEmpty && alreadyFound.isEmpty) {
      cliques.add(potentialClique)
    }
    candidates.foreach { candidateVertex =>
      {

        val neighbourVertices = graph.collectNeighborIds(EdgeDirection.Either)
          .filter(_._1 == candidateVertex).collect().head._2.toSet;

        findCliques(potentialClique ++ MutableSet(candidateVertex),
          candidates.intersect(neighbourVertices),
          alreadyFound.intersect(neighbourVertices),
          cliques)
      }
    }
  }

}