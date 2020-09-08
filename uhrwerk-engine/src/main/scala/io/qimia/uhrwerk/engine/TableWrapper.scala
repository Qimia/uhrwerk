package io.qimia.uhrwerk.engine

import java.nio.file.Path
import java.time.LocalDateTime
import java.util.concurrent.Executors

import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService
import io.qimia.uhrwerk.common.model.{Partition, PartitionUnit, Table, Target}
import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.engine.Environment.Ident
import io.qimia.uhrwerk.engine.tools.{
  DependencyHelper,
  SourceHelper,
  TimeHelper
}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, duration}

object TableWrapper {
  def createPartitions(partitions: Array[LocalDateTime],
                       partitionUnit: PartitionUnit,
                       partitionSize: Int,
                       targetId: Long): Array[Partition] = {
    partitions.map(t => {
      val newPart = new Partition()
      newPart.setPartitionTs(t)
      newPart.setPartitionSize(partitionSize)
      newPart.setPartitionUnit(partitionUnit)
      newPart.setTargetId(targetId)
      newPart.setKey()
      newPart
    })
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
    * (Relies on the dependencyResults for getting the dependencies and on the startTS and endTSExcl to get the sources
    * and write the right partitions
    * @param dependencyResults a list with for each dependency which partitions need to be loaded
    * @param partitionTS a list of partition starting timestamps
    */
  private def singleRun(
      dependencyResults: List[BulkDependencyResult],
      partitionTS: Array[LocalDateTime]): Boolean = {
    // TODO: Log start of single task for table here
    val startTs = partitionTS.head
    val endTs = {
      val lastInclusivePartitionTs = partitionTS.last
      val tableDuration = TimeHelper.convertToDuration(table.getPartitionUnit,
        table.getPartitionSize)
      Option(lastInclusivePartitionTs.plus(tableDuration))
    }
    println("Start of Single Run")
    println(s"TS: ${startTs} Optional end-TS: ${endTs}")

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
              frameManager.loadSourceDataFrame(s, Option(startTs), endTs)
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
      if (!frame.isEmpty) {
        frameManager.writeDataFrame(frame, table, partitionTS)
      }
      true
    } catch {
      case e: Throwable => {
        System.err.println("Task failed: " + startTs.toString)
        e.printStackTrace()
        false
      }
    }
    println("End of Single Run")
    // TODO: Proper logging here
    success
  }

  /**
    * Process table for given array of partition (start) datetimes
    * @param partitionsTs Array of localdatetime denoting the starttimes of the partitions
    * @param ex execution context onto which the futures are created
    * @return list of futures for the started tasks
    */
  def runTasks(partitionsTs: Array[LocalDateTime], overwrite: Boolean = false)(
      implicit ex: ExecutionContext): List[Future[Boolean]] = {
    val dependencyRes =
      metastore.tableDependencyService.processingPartitions(table, partitionsTs)
    println(s"Failed partitionTs: ${dependencyRes.getFailedTs.mkString(", ")}")

    // If no tasks to be done => quit
    val taskTimes = dependencyRes.getResolvedTs
    if ((taskTimes == null) || (taskTimes.isEmpty)) {
      println("No tasks ready to run")
      return Nil
    }

    // TODO: Reporting of Missing LocalDateTime?

    val groups = DependencyHelper.createTablePartitionResultGroups(
      dependencyRes,
      tableDuration,
      table.getMaxBulkSize)
    val tasks = groups.map(partitionGroup => {
      val localGroupTs = partitionGroup.map(_.getPartitionTs)
      val bulkInput =
        DependencyHelper.extractBulkDependencyResult(partitionGroup)
      println(s"BulkInput length: ${bulkInput.length}")
      Future {
        val res = singleRun(bulkInput, localGroupTs)
        if (res) {
          table.getTargets.foreach((t: Target) => {
            val partitions =
              TableWrapper.createPartitions(localGroupTs,
                                            table.getPartitionUnit,
                                            table.getPartitionSize,
                                            t.getId)
            val _ = metastore.partitionService.save(partitions, overwrite)
            // TODO: Need to handle failure to store
          })
        }
        res
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

  /**
    * Utility function to create an execution context and block until runTasks is done processing the batches.
    * See [[io.qimia.uhrwerk.engine.TableWrapper#runTasks]]
    * @param startTimes sequence of partitionTS which need to be processed
    * @param threads number of threads used by the threadpool
    */
  def runTasksAndWait(startTimes: Array[LocalDateTime],
                      overwrite: Boolean = false,
                      threads: Option[Int] = Option.empty): List[Boolean] = {

    val executor = if (threads.isEmpty) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads.get)
    }
    implicit val executionContext = ExecutionContext.fromExecutor(executor)
    val futures = runTasks(startTimes, overwrite)
    val result = Await.result(Future.sequence(futures),
                 duration.Duration(24, duration.HOURS))
    executor.shutdown()
    result
  }

}
