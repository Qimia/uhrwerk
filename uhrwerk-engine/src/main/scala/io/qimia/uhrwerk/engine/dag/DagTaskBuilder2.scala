package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.model.PartitionTransformType
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}
import io.qimia.uhrwerk.engine.Environment.{TableIdent, getTableIdent, tableCleaner}

import java.time.LocalDateTime
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable

class DagTaskBuilder2(environment: Environment) {

  /**
    * Create a map with all DagTasks and their dependencies. It does not do any optimization on this dag yet
    * (no bulking). This dag can be executed by running the tasks without requirements. The final tables are the ones
    * which do not have any upstream tasks.
    *
    * @param outTable table which needs to be generated
    * @param startTs  inclusive start timestamp of first partition
    * @param endTs    exclusive end timestamp of last partition
    * @return List of TableWrappers and when they need to run.
    */
  def buildTaskListFromTable(outTable: TableWrapper, startTs: LocalDateTime, endTs: LocalDateTime): Map[DT2Key, DT2] = {
    val callTime = LocalDateTime.now()

    def recursiveBuild(targetTaskKey: DT2Key,
                       upstreamTask: Option[DT2Key],
                       mapState: Map[DT2Key, DT2]): Map[DT2Key, DT2] = {

      // first start with adding current task to the map (or update existing task)
      if (mapState.contains(targetTaskKey)) {
        val targetTask = mapState(targetTaskKey)
        if (upstreamTask.isDefined) {
          targetTask.upstreamDependencies += upstreamTask.get
        }
        mapState
      } else {
        val wrap               = environment.getTable(targetTaskKey.ident).get
        val upstreamDependants = new mutable.ListBuffer[DT2Key]
        if (upstreamTask.isDefined) {
          upstreamDependants.append(upstreamTask.get)
        }
        // TODO Figure out what dependencies this target table needs
        val missingDeps: mutable.Set[DT2Key] = wrap.wrappedTable.getDependencies
          .map(d => {
            val ident       = TableIdent(d.getArea, d.getVertical, d.getTableName, d.getVersion)
            val targetTimes = targetTaskKey.partition :: Nil
            val times = d.getTransformType match {
              case PartitionTransformType.IDENTITY => targetTimes
              case PartitionTransformType.WINDOW =>
                TimeTools.convertToWindowBatchList(targetTimes, wrap.tableDuration, d.getTransformPartitionSize)
              case PartitionTransformType.AGGREGATE =>
                TimeTools.convertToSmallerBatchList(targetTimes, wrap.tableDuration, d.getTransformPartitionSize)
              case PartitionTransformType.NONE => callTime :: Nil // None's can only run at current time
            }
            times.map(DT2Key(ident, _))
          })
          .foldLeft(new mutable.HashSet[DT2Key])((s, keys) => s ++ keys)
        val newDT2     = new DT2(wrap, ListBuffer(targetTaskKey.partition), missingDeps, upstreamDependants)
        val updatedMap = mapState + (targetTaskKey -> newDT2)

        // Let's do this for every
        if (missingDeps.nonEmpty) {
          missingDeps
            .map((k: DT2Key) => (inMap: Map[DT2Key, DT2]) => recursiveBuild(k, Option(targetTaskKey), inMap))
            .reduce(_ andThen _)(updatedMap)
        } else {
          updatedMap
        }
      }
    }

    // If it's unpartitioned only run for the calltime
    if (!outTable.wrappedTable.isPartitioned) {
      return recursiveBuild(DT2Key(getTableIdent(outTable.wrappedTable), callTime),
                            Option.empty,
                            new immutable.HashMap[DT2Key, DT2])
    }

    val partitionTs = TimeTools
      .convertRangeToBatch(startTs, endTs, outTable.tableDuration)

    val processedPartitions = environment.metaStore.tableDependencyService
      .processingPartitions(outTable.wrappedTable, partitionTs.toArray)
      .getProcessedTs
      .toSet

    // We skip the bulking step until later
    val partitionToDoTs = partitionTs.filter(p => !processedPartitions.contains(p))
    val currentKeys     = partitionToDoTs.map(t => DT2Key(Environment.getTableIdent(outTable.wrappedTable), t))
    val emptyMap        = new immutable.HashMap[DT2Key, DT2]
    currentKeys
      .map((k: DT2Key) => (inMap: Map[DT2Key, DT2]) => recursiveBuild(k, Option.empty, inMap))
      .reduce(_ andThen _)(emptyMap)
  }

}
