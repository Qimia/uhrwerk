package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.metastore.model.{
  DependencyModel,
  Partition,
  TableModel
}
import io.qimia.uhrwerk.engine.Environment.TableIdent
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}
import org.apache.log4j.Logger
import org.jgrapht.graph.{DefaultDirectedGraph, DefaultEdge}

import java.time.LocalDateTime
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

  private val logger: Logger = Logger.getLogger(this.getClass)

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

    val dag = new DefaultDirectedGraph[(TableWrapper, Boolean), DefaultEdge](
      classOf[DefaultEdge]
    )

    def buildGraph(
        tPair: (TableWrapper, Boolean),
        dag: DefaultDirectedGraph[(TableWrapper, Boolean), DefaultEdge]
    ) {
      dag.addVertex(tPair)
      val aTableId = tPair._1.wrappedTable.getId
      val aTable = environment.getTableById(aTableId).get
      if (
        aTable.wrappedTable.getDependencies != null
        && !aTable.wrappedTable.getDependencies.isEmpty
      )
        aTable.wrappedTable.getDependencies
          .foreach(dependency => {
            val depTable =
              environment.getTableById(dependency.getDependencyTableId).get
            val partitions = lastPartitions(
              depTable.wrappedTable,
              dependency,
              environment.jobProperties
            )
            val depPair = (depTable, partitions.nonEmpty)
            buildGraph(depPair, dag)
            dag.addEdge(depPair, tPair)
          })
    }

    buildGraph((outTable, false), dag)

    def pTraverse(
        node: (TableWrapper, Boolean),
        dag: DefaultDirectedGraph[(TableWrapper, Boolean), DefaultEdge],
        nDag: mutable.LinkedHashSet[(TableWrapper, Boolean)]
    ): (TableWrapper, Boolean) = {
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
    val nDag = mutable.LinkedHashSet[(TableWrapper, Boolean)]()

    pTraverse((outTable, false), dag, nDag)

    nDag
      .filter(!_._2)
      .map(node =>
        DagTask(
          node._1,
          List(callTime)
        )
      )
      .toList
  }

  private def lastPartitions(
      depTable: TableModel,
      dep: DependencyModel,
      properties: mutable.Map[String, AnyRef]
  ): List[Partition] = {
    if (
      depTable.getPartitionColumns() != null
      && !depTable.getPartitionColumns().isEmpty
    ) {
      logger.debug(
        s"### Dependency-Table:$depTable"
      )
      logger.debug(
        s"### Dependency:$dep"
      )
      if (
        dep.getPartitionMappings() != null
        && !dep.getPartitionMappings().isEmpty
      ) {
        val mappings = dep.getPartitionMappings.asScala
          .map(mapping => {
            val column = mapping._1
            val value = mapping._2
            var propValue = value

            if (value != null && value.isInstanceOf[String]) {
              val valueStr = value.asInstanceOf[String]
              if (valueStr.startsWith("$") && valueStr.endsWith("$")) {
                val propName = valueStr.substring(1, valueStr.length - 1)
                val opt = properties.get(propName)
                if (opt.isDefined)
                  propValue = opt.get
                else
                  throw new IllegalArgumentException(
                    s"Property $propName not found in properties ${properties.keys.mkString(", ")}"
                  )
              }
            }
            (column, propValue)
          })
          .toMap
        return environment.metaStore.partitionService
          .getLatestPartitions(dep.getDependencyTargetId, mappings.asJava)
          .asScala
          .toList
      }
    }
    val partition = environment.metaStore.partitionService
      .getLatestPartition(depTable.getTargets()(0).getId)
    if (partition == null) List()
    else
      List(partition)
  }
}
