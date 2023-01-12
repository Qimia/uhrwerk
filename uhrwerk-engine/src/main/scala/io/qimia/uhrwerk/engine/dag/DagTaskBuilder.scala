package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}
//import org.apache.hadoop.thirdparty.com.google.common.graph.{GraphBuilder, MutableGraph}
import org.apache.hadoop.shaded.com.google.common.graph.{
  GraphBuilder,
  MutableGraph
}

import java.time.LocalDateTime
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object DagTaskBuilder {

  /** Filter distinct Dag tasks only by table and partitions requested
    * This is required because DagTask contains dept info which trips up SeqOps.distinct
    * @param tasks a sequence of DagTasks (meaning a tablewrapper with some partitions needed for that table)
    * @return a sequence of DagTasks with all duplicate table-partition pairs filtered out (removing later one)
    */
  def distinctDagTasks(tasks: Seq[DagTask]): Seq[DagTask] = {
    val builder: ListBuffer[DagTask] = ListBuffer()
    val seen = mutable.HashSet.empty[(TableWrapper, Seq[LocalDateTime])]
    val it = tasks.iterator
    var different = false
    while (it.hasNext) {
      val next = it.next()
      if (seen.add(next.table, next.partitions)) {
        builder += next
      } else {
        different = true
      }
    }
    if (different) {
      builder.toList
    } else {
      tasks
    }
  }
}

class DagTaskBuilder(environment: Environment) {

  /** Create a queue of table + partition-list which need to be present for filling a given output table for a
    * particular time-range.
    * The queue is filling an bulk partition section of the outTable at a time.
    * @param outTable table which needs to be generated
    * @param startTs inclusive start timestamp of first partition
    * @param endTs exclusive end timestamp of last partition
    * @return List of TableWrappers and when they need to run.
    */
  def buildTaskListFromTable(outTable: TableWrapper): List[DagTask] = {
    val callTime = LocalDateTime.now()

    val dag: MutableGraph[(TableIdent, Boolean)] =
      GraphBuilder
        .directed()
        .build()

    def buildGraph(
        tPair: (TableIdent, Boolean),
        dag: MutableGraph[(TableIdent, Boolean)]
    ) {
      val tIdent = tPair._1
      val aTable = environment.getTable(tIdent).get
      val dependencyTables =
        if (
          aTable.wrappedTable.getDependencies != null
          && !aTable.wrappedTable.getDependencies.isEmpty
        )
          aTable.wrappedTable.getDependencies
            .foreach(d => {
              val depIdent =
                new TableIdent(
                  d.getArea,
                  d.getVertical,
                  d.getTableName,
                  d.getVersion
                )
              val depTable = environment.getTable(depIdent).get
              val partition = environment.metaStore.partitionService
                .getLatestPartition(depTable.wrappedTable.getTargets()(0).getId)
              val depPair = (depIdent, partition != null)
              dag.putEdge(depPair, tPair)
              buildGraph(depPair, dag)
            })
    }

    val outWrappedTable = outTable.wrappedTable
    val outIdent = new TableIdent(
      outWrappedTable.getArea,
      outWrappedTable.getVertical,
      outWrappedTable.getName,
      outWrappedTable.getVersion
    )

    buildGraph((outIdent, false), dag)

    def pTraverse(
        node: (TableIdent, Boolean),
        dag: MutableGraph[(TableIdent, Boolean)],
        nDag: MutableGraph[(TableIdent, Boolean)]
    ): (TableIdent, Boolean) = {
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

    val nDag: MutableGraph[(TableIdent, Boolean)] =
      GraphBuilder
        .directed()
        .build()

    pTraverse((outIdent, false), dag, nDag)

    def topSortGraph(
        node: (TableIdent, Boolean),
        dag: MutableGraph[(TableIdent, Boolean)]
    ): List[(TableIdent, Boolean)] = {
      val preNodes = dag.predecessors(node).asScala
      if (preNodes.isEmpty) {
        List(node)
      } else {
        preNodes
          .map(topSortGraph(_, dag))
          .reduce((l, r) => l ::: r) ::: List(node)
      }
    }
    val sorted = topSortGraph((outIdent, false), nDag)
    val filtered = sorted.filter(!_._2)
    filtered.map(node =>
      DagTask(
        environment.getTable(node._1).get,
        List(callTime),
        nDag.degree(node)
      )
    )
  }

}
