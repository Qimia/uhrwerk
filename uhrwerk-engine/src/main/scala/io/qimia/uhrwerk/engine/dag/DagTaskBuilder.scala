package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.metastore.model.{
  DependencyModel,
  Partition,
  TableModel
}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}

import java.time.LocalDateTime
import java.util.Properties
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

    val dag = new DefaultDirectedGraph[(TableIdent, Boolean), DefaultEdge](
      classOf[DefaultEdge]
    )

    def buildGraph(
        tPair: (TableIdent, Boolean),
        dag: DefaultDirectedGraph[(TableIdent, Boolean), DefaultEdge]
    ) {
      dag.addVertex(tPair)
      val tIdent = tPair._1
      val aTable = environment.getTable(tIdent).get
      val dependencyTables =
        if (
          aTable.wrappedTable.getDependencies != null
          && !aTable.wrappedTable.getDependencies.isEmpty
        )
          aTable.wrappedTable.getDependencies
            .foreach(dependency => {
              val depIdent =
                new TableIdent(
                  dependency.getArea,
                  dependency.getVertical,
                  dependency.getTableName,
                  dependency.getVersion
                )
              val depTable = environment.getTable(depIdent).get
              val partitions = lastPartitions(
                depTable.wrappedTable,
                dependency,
                environment.jobProperties
              )
              val depPair = (depIdent, !partitions.isEmpty)
              buildGraph(depPair, dag)
              dag.addEdge(depPair, tPair)
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
        dag: DefaultDirectedGraph[(TableIdent, Boolean), DefaultEdge],
        nDag: mutable.LinkedHashSet[(TableIdent, Boolean)]
    ): (TableIdent, Boolean) = {
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
    val nDag = mutable.LinkedHashSet[(TableIdent, Boolean)]()

    pTraverse((outIdent, false), dag, nDag)

    nDag
      .filter(!_._2)
      .map(node =>
        DagTask(
          environment.getTable(node._1).get,
          List(callTime)
        )
      )
      .toList
  }

  private def lastPartitions(
      depTable: TableModel,
      dep: DependencyModel,
      properties: Properties
  ): List[Partition] = {
    if (
      depTable.getPartitionColumns != null && !depTable.getPartitionColumns.isEmpty
    ) {
      if (
        dep.getPartitionMappings != null || !dep.getPartitionMappings.isEmpty
      ) {
        val mappings = dep.getPartitionMappings.asScala
          .map(mapping => {
            val k = mapping._1
            val v = mapping._2
            var propVal = v

            if (v.isInstanceOf[String]) {
              val str = v.asInstanceOf[String]
              if (str.startsWith("$") && str.endsWith("$"))
                propVal = properties.getProperty(
                  str.substring(1, str.length - 1)
                )
              if (propVal == null)
                throw new RuntimeException(
                  s"Property ${str.substring(1, str.length - 1)} not found"
                )
            }
            (k, propVal)
          })
          .toMap
        return environment.metaStore.partitionService
          .getLatestPartitions(depTable.getTargets()(0).getId, mappings.asJava)
          .asScala
          .toList
      }
    }
    val partition = environment.metaStore.partitionService
      .getLatestPartition(depTable.getTargets()(0).getId)
    List(partition)
  }
}
