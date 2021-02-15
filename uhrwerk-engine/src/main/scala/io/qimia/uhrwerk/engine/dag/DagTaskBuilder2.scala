package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.common.model.{PartitionTransformType, Table}
import io.qimia.uhrwerk.common.tools.TimeTools
import io.qimia.uhrwerk.engine.{Environment, TableWrapper}
import io.qimia.uhrwerk.engine.Environment.{TableIdent, getTableIdent, tableCleaner}
import io.qimia.uhrwerk.engine.tools.TimeHelper

import java.time.LocalDateTime
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.immutable

object DagTaskBuilder2 {

  /**
   * Combine multiple DagTasks into a single DagTask by combining it's elements (dependencies, partitions, upstream)
   * @param tasks List of DagTasks
   * @return A single merged DagTask
   */
  def mergeDagTask(tasks: List[DT2]): DT2 = {
    val endTask = tasks.head
    tasks.tail.foreach(t => {
      endTask.missingDependencies ++= t.missingDependencies
      endTask.upstreamDependencies ++= t.upstreamDependencies

      // There should not be any overlap possible
      endTask.partitions ++= t.partitions
    })
    endTask
  }

  /**
   * Apply bulk optimization for a Dag on the level of a single table.
   * It will try to merge adjacent DagTasks together upto the MaxBulkSize.
   * Keys will all point to the same DagTask at that point (for one bulk task).
   * @param tableWrapper tableWrapper for the Dag-level's table
   * @param tasks A list of key-value pairs that belong to this single table
   * @return A list of key-value pairs which have been bulk-optimized
   */
  def bulkOptimizeTable(tableWrapper: TableWrapper, tasks: List[(DT2Key, DT2)]): List[(DT2Key, DT2)] = {
    val sortedList = tasks.sortWith((a: (DT2Key, DT2), b: (DT2Key, DT2)) => a._1.partition.isBefore(b._1.partition))
    val bulkTimeSplits = TimeHelper.groupSequentialIncreasing(sortedList.map(_._1.partition),
                                                              tableWrapper.tableDuration,
                                                              tableWrapper.wrappedTable.getMaxBulkSize)
    val res: mutable.ListBuffer[(DT2Key, DT2)] = new mutable.ListBuffer
    var leftTableTasks = sortedList
    bulkTimeSplits.foreach(splitNum => {
      val (bulkGroup, rest) = leftTableTasks.splitAt(splitNum)
      val (keys, tasks) = bulkGroup.unzip
      val bulkTask = mergeDagTask(tasks)
      res.appendAll(keys.map(k => k -> bulkTask))
      leftTableTasks = rest
    })
    res.toList
  }

  /**
   * Apply a bulk-optimization step to the DagTask-map. This will put together tasks which can be combined according
   * to the MaxBulk setting. Different TaskKeys will refer to the same task which combines the work.
   * @param taskmap an Dag with only single-partition tasks
   * @return TaskMap with bulk optimizations
   */
  def bulkOptimizeTaskmap(taskmap: Map[DT2Key, DT2]): Map[DT2Key, DT2] = {
    val tableGroups = taskmap.toList
      .groupBy((tup: (DT2Key, DT2)) => Environment.getTableIdent(tup._2.tableWrapper.wrappedTable))
      .values

    tableGroups
      .map(g => {
        val tableWrap = g.head._2.tableWrapper
        if (tableWrap.wrappedTable.getMaxBulkSize < 2) {
          g
        } else {
          bulkOptimizeTable(tableWrap, g)
        }
      })
      .map(_.toMap)
      .reduce((map1, map2) => map1 ++ map2)
  }

  /**
   * Merge task lists together for when multiple output tables need to be combined in a single dag.
   * Should be used before bulk-optimization! It will update the unpartitioned runtimes to the one used in taskmapA.
   * @param taskmapA a taskmap for a dag with a particular target table
   * @param taskmapB a taskmap with a dag for a different target table
   * @return combined Map
   */
  def mergeTaskLists(taskmapA: Map[DT2Key, DT2], taskmapB: Map[DT2Key, DT2]): Map[DT2Key, DT2] = {
    val unpartitionedTimeA = taskmapA.values.filterNot(task => task.tableWrapper.wrappedTable.isPartitioned).head.partitions.head
    val BParts = taskmapB.groupBy(tup => taskmapA.contains(tup._1))

    // tasks which are in both
    BParts.filter(tup => tup._1).head._2.foreach((kv: (DT2Key, DT2)) => {
      val taskA = taskmapA(kv._1)
      assert(taskA.partitions.size == 1)
      assert(kv._2.partitions.size == 1)
      taskA.upstreamDependencies ++= kv._2.upstreamDependencies
    })

    val notFoundParts = BParts.filterNot(tup => tup._1).head._2
    val BNotFoundByPartition = notFoundParts.groupBy(kv => kv._2.tableWrapper.wrappedTable.isPartitioned)

    val unpartitionedUpdates = BNotFoundByPartition.map(partTuple => {
      if (partTuple._1) {
        partTuple._2
      } else {
        partTuple._2.map(kv => {
          assert(kv._2.partitions.size == 1)
          kv._2.partitions.remove(0)
          kv._2.partitions.append(unpartitionedTimeA)
          kv._1 -> kv._2
        })
      }
    }).toList
    // TODO: UNTESTED (!!)
    (taskmapA :: unpartitionedUpdates).reduce(_ ++ _)
  }
}

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
        val upstreamDependants = new mutable.HashSet[DT2Key]
        if (upstreamTask.isDefined) {
          upstreamDependants.add(upstreamTask.get)
        }
        // TODO Figure out what dependencies this target table needs
        val missingDeps: mutable.Set[DT2Key] = tableCleaner(wrap.wrappedTable).getDependencies
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
