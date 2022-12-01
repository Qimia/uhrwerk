package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import java.time.LocalDateTime
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.engine.Environment.{TableIdent, tableCleaner}
import io.qimia.uhrwerk.engine.tools.TimeHelper
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import scala.collection.JavaConverters._

object DagTaskBuilder {

  /**
    * Filter distinct Dag tasks only by table and partitions requested
    * This is required because DagTask contains dept info which trips up SeqOps.distinct
    * @param tasks a sequence of DagTasks (meaning a tablewrapper with some partitions needed for that table)
    * @return a sequence of DagTasks with all duplicate table-partition pairs filtered out (removing later one)
    */
  def distinctDagTasks(tasks: Seq[DagTask]): Seq[DagTask] = {
    val builder: ListBuffer[DagTask] = ListBuffer()
    val seen                         = mutable.HashSet.empty[(TableWrapper, Seq[LocalDateTime])]
    val it                           = tasks.iterator
    var different                    = false
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

  /**
    * Create a queue of table + partition-list which need to be present for filling a given output table for a
    * particular time-range.
    * The queue is filling an bulk partition section of the outTable at a time.
    * @param outTable table which needs to be generated
    * @param startTs inclusive start timestamp of first partition
    * @param endTs exclusive end timestamp of last partition
    * @return List of TableWrappers and when they need to run.
    */
  def buildTaskListFromTable(outTable: TableWrapper, startTs: LocalDateTime, endTs: LocalDateTime): List[DagTask] = {
    val callTime = LocalDateTime.now()

    def recursiveBuild(aTable: TableWrapper, partitionTimes: List[LocalDateTime], dept: Int = 0): List[DagTask] = {
      val dependencyTables = tableCleaner(aTable.wrappedTable).getDependencies
        .map(d => {
          val ident    = TableIdent(d.getArea, d.getVertical, d.getTableName, d.getVersion)
          val depTable = environment.getTable(ident).get
          val times = partitionTimes
          (depTable, times)
        })
        .flatMap(tup => recursiveBuild(tup._1, tup._2, dept + 1))
      DagTask(aTable, partitionTimes, dept) :: dependencyTables.toList
    }

    // If it's unpartitioned only run for the calltime
    if (!outTable.wrappedTable.getPartitioned) {
      return recursiveBuild(outTable, List(callTime)).reverse
    }

    val partitionTs = TimeTools
      .convertRangeToBatch(startTs, endTs, outTable.tableDuration)

    val processedPartitions = environment.metaStore.tableDependencyService
      .processingPartitions(outTable.wrappedTable, partitionTs.asJava)
      .getProcessedTs
      .toSet

    val partitionBulkTs = TimeHelper.createPartitionBulkGroups(
      partitionTs
        .filter(p => !processedPartitions.contains(p)),
      outTable.tableDuration,
      outTable.wrappedTable.getMaxBulkSize
    )
    partitionBulkTs
      .flatMap((bulkTs: List[LocalDateTime]) => {
        // Reverse to do dependencies before target-tables
        recursiveBuild(outTable, bulkTs).reverse
      })
  }

}
