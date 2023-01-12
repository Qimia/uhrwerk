package io.qimia.uhrwerk.engine.dag

import org.apache.hadoop.shaded.com.google.common.graph.{GraphBuilder, MutableGraph}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._

class DirectedGraphTest extends AnyFlatSpec {

  it should "work" in {

    val dag: MutableGraph[(String, Boolean)] =
      GraphBuilder
        .directed()
        .build()
    dag.putEdge(("B", true), ("A", false))
    dag.putEdge(("C", true), ("B", true))
    dag.putEdge(("D", false), ("C", true))
    dag.putEdge(("E", true), ("C", true))

    val nodes = dag.nodes().asScala
    nodes.foreach(println)

    val nDag: MutableGraph[(String, Boolean)] =
      GraphBuilder
        .directed()
        .build()

    def pTraverse(
        node: (String, Boolean),
        dag: MutableGraph[(String, Boolean)],
        nDag: MutableGraph[(String, Boolean)]
    ): (String, Boolean) = {
      val preNodes = dag.predecessors(node).asScala
      if (preNodes.isEmpty) {
        node
      } else {
        val tpNodes = preNodes.map(pTraverse(_, dag, nDag))
        val process = node._2 && tpNodes.map(_._2).reduce((l, r) => l && r)
        val nwNode = (node._1, process)
        tpNodes.foreach(nDag.putEdge(_, nwNode))
        nwNode
      }
    }

    pTraverse(("A", false), dag, nDag)
    println("##### traversed dags")
    val nodes2 = nDag.nodes().asScala
    nodes2.foreach(println)

    def topSortGraph(
        node: (String, Boolean),
        dag: MutableGraph[(String, Boolean)]
    ): List[(String, Boolean)] = {
      val preNodes = dag.predecessors(node).asScala
      if (preNodes.isEmpty) {
        List(node)
      } else {
        preNodes
          .map(topSortGraph(_, dag))
          .reduce((l, r) => l ::: r) ::: List(node)
      }
    }

    val sorted = topSortGraph(
      nodes2.find(_._1.equals("A")).get,
      nDag
    )

    println("Topological Sorted")
    sorted.foreach(println)

    println("Nodes to Process")
    sorted.filter(!_._2).foreach(println)


  }
}
