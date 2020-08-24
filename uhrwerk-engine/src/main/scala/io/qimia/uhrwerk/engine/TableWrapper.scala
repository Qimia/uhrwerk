package io.qimia.uhrwerk.engine

import java.nio.file.Path
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService
import io.qimia.uhrwerk.common.model.{Partition, Table}
import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.engine.Environment.Ident
import io.qimia.uhrwerk.engine.tools.{
  DependencyHelper,
  SourceHelper,
  TimeHelper
}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object TableWrapper {
  def createPartitions(): Array[Partition] = {
    val part = new Partition
    Array(part)
  }
}

class TableWrapper(metastore: MetaStore,
                   table: Table,
                   userFunc: TaskInput => DataFrame,
                   frameManager: FrameManager) {

  val tableDuration =
    TimeHelper.convertToDuration(table.getPartitionUnit, table.getPartitionSize)

  /**
    * Single invocation of usercode (possibly bulk-modus)
    * @param dependencyResults a list with for each dependency which partitions need to be loaded
    * @param startTS start timestamp
    * @param endTSExcl end timestamp exclusive
    */
  private def singleRun(
      dependencyResults: List[BulkDependencyResult],
      startTS: LocalDateTime,
      endTSExcl: Option[LocalDateTime] = Option.empty): Boolean = {
    // TODO: Log start of single task for table here

    val inputDepDFs: List[(Ident, DataFrame)] =
      if (dependencyResults.nonEmpty) {
        dependencyResults.map(bd => {
          val df = frameManager.loadDependencyDataFrame(bd)
          val id = DependencyHelper.extractTableIdentity(bd)
          id -> df
        })
        //val df: DataFrame = null
      } else {
        Nil
      }
    val sources = table.getSources
    val inputSourceDFs: List[(Ident, DataFrame)] =
      if ((sources != null) && sources.nonEmpty) {
        sources
          .map(s => {
            val df =
              frameManager.loadSourceDataFrame(s, Option(startTS), endTSExcl)
            val id = SourceHelper.extractSourceIdent(s)
            id -> df
          })
          .toList
      } else {
        Nil
      }
    val inputMap = (inputDepDFs ::: inputSourceDFs).toMap
    val taskInput = TaskInput(inputMap)

    val success = try {
      val frame = userFunc(taskInput)
      // TODO error checking: if target should be on datalake but no frame is given
      // Note: We are responsible for all standard writing of DataFrames
      frameManager.writeDataFrame(frame, table)
      true
    } catch {
      case e: Throwable => {
        System.err.println("Task failed: " + startTS.toString)
        e.printStackTrace()
        false
      }
    }
    // TODO: Log Success for metastore here (and write partitions here)
    success
  }

  /**
    * Process table for given array of partition (start) datetimes
    * @param partitions Array of localdatetime denoting the starttimes of the partitions
    * @param ex execution context onto which the futures are created
    * @return list of futures for the started tasks
    */
  def runTasks(partitions: Array[LocalDateTime])(
      implicit ex: ExecutionContext): List[Future[Unit]] = {
    val dependencyRes =
      metastore.tableDependencyService.processingPartitions(table, partitions)

    // TODO: Reporting of Missing LocalDateTime?

    val groups = DependencyHelper.createTablePartitionResultGroups(
      dependencyRes,
      tableDuration,
      table.getMaxBulkSize)
    val tasks = groups.map(partitionGroup => {
      val bulkInput =
        DependencyHelper.extractBulkDependencyResult(partitionGroup)
      val startTs = partitionGroup.head.getPartitionTs
      val endTs = if (partitionGroup.length > 1) {
        Option(partitionGroup.last.getPartitionTs)
      } else {
        Option.empty
      }
      Future {
        val res = singleRun(bulkInput, startTs, endTs)
        if (res) {
          val partitions = TableWrapper.createPartitions()
          val _ = metastore.partitionService.save(partitions, true)
          // TODO: Overwrite hardcoded on at the moment
          // TODO: Need to handle failure to store
        }
      }
    })

    /**
      * Idea: Give a complete report at the end after calling runTasks
      * Showing partitions already there, partitions with missing dependencies
      * and if anything failed or went wrong while producing certain partitions
      * (this also means giving the user more info there)
      */
    tasks
  }

}
