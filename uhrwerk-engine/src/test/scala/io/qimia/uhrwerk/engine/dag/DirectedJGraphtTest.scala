package io.qimia.uhrwerk.engine.dag

import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

class DirectedJGraphtTest extends AnyFlatSpec {

  it should "work" in {

    val dag = new DefaultDirectedGraph[(String, Boolean), DefaultEdge](
      classOf[DefaultEdge]
    )

    dag.addVertex(("A", false))
    dag.addVertex(("B", true))
    dag.addEdge(("B", true), ("A", false))

    dag.addVertex(("C", true))
    dag.addEdge(("C", true), ("B", true))

    dag.addVertex(("D", false))
    dag.addEdge(("D", false), ("C", true))
    dag.addVertex(("E", true))
    dag.addEdge(("E", true), ("C", true))

    val nodes = dag.vertexSet().asScala
    nodes.foreach(println)

    val nDag =
      new DefaultDirectedGraph[(String, Boolean), DefaultEdge](
        classOf[DefaultEdge]
      )

    def pTraverse(
        node: (String, Boolean),
        dag: DefaultDirectedGraph[(String, Boolean), DefaultEdge],
        nDag: DefaultDirectedGraph[(String, Boolean), DefaultEdge]
    ): (String, Boolean) = {
      val preNodes = dag.incomingEdgesOf(node).asScala.map(dag.getEdgeSource)
      if (preNodes.isEmpty) {
        nDag.addVertex(node)
        node
      } else {
        val tpNodes = preNodes.map(pTraverse(_, dag, nDag))
        val process = node._2 && tpNodes.map(_._2).reduce((l, r) => l && r)
        val nwNode = (node._1, process)
        nDag.addVertex(nwNode)
        tpNodes.foreach(nDag.addEdge(_, nwNode))
        nwNode
      }
    }

    pTraverse(("A", false), dag, nDag)
    println("##### traversed dags")
    val sorted = nDag.vertexSet().asScala
    sorted.foreach(println)

    println("Nodes to Process")
    sorted.filter(!_._2).foreach(println)

  }
}
