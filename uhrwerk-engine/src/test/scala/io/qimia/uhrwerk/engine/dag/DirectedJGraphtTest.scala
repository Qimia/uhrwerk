package io.qimia.uhrwerk.engine.dag

import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._
import scala.collection.mutable

class DirectedJGraphtTest extends AnyFlatSpec {

  it should "preserve the order" in {
    val linkedSet = mutable.LinkedHashSet[(String,Boolean)]()

    linkedSet.add("A" -> true)
    linkedSet.add("B" -> false)
    linkedSet.add("C" -> false)

    linkedSet.foreach(println)
    linkedSet.filter(!_._2).foreach(println)

  }

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

    val list = mutable.LinkedHashSet[(String, Boolean)]()

    def pTraverse(
        node: (String, Boolean),
        dag: DefaultDirectedGraph[(String, Boolean), DefaultEdge],
        nDag: mutable.LinkedHashSet[(String, Boolean)]
    ): (String, Boolean) = {
      val preNodes = dag.incomingEdgesOf(node).asScala.map(dag.getEdgeSource)
      if (preNodes.isEmpty) {
        nDag.add(node)
        node
      } else {
        val tpNodes = preNodes.map(pTraverse(_, dag, nDag))
        val process = node._2 && tpNodes.map(_._2).reduce((l, r) => l && r)
        val nwNode = (node._1, process)
        nDag.add(nwNode)
        nwNode
      }
    }

    pTraverse(("A", false), dag, list)
    println("##### traversed dags")
    list.foreach(println)

    println("Nodes to Process")
    list.filter(!_._2).toList.foreach(println)

  }
}
