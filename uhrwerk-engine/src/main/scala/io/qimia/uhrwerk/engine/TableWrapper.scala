package io.qimia.uhrwerk.engine

import java.time.{Duration, LocalDateTime}
import java.util.concurrent.Executors

import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.common.metastore.dependency.{TablePartitionResult, TablePartitionResultSet}
import io.qimia.uhrwerk.common.model.{Partition, PartitionUnit, Table, Target}
import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent}
import io.qimia.uhrwerk.engine.tools.{DependencyHelper, SourceHelper, TimeHelper}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.concurrent._

object TableWrapper {
  /**
   * Construct an array of partitions for a particular target
   * @param partitions for which datetimes to create partitions (startTime of partition)
   * @param partitioned set to false if partitions belong to a "snapshot table"
   * @param partitionUnit partition-duration: for which length of time
   * @param partitionSize partition-duration: how many times that length of time
   * @param targetId id of target (to which the partitions belong)
   * @return Array of partition objects
   */
  def createPartitions(
      partitions: Array[LocalDateTime],
      partitioned: Boolean,
      partitionUnit: PartitionUnit,
      partitionSize: Int,
      targetId: Long
  ): Array[Partition] = {
    partitions.map(t => {
      val newPart = new Partition()
      newPart.setPartitionTs(t)
      newPart.setPartitionSize(partitionSize)
      newPart.setPartitionUnit(partitionUnit) // Warning: Sets null directly from table object
      newPart.setTargetId(targetId)
      newPart.setKey()
      newPart
    })
  }
}

class TableWrapper(metastore: MetaStore, table: Table, userFunc: TaskInput => TaskOutput, frameManager: FrameManager) {
  val wrappedTable = table
  val tableDuration: Duration = if (table.isPartitioned) {
    TimeHelper.convertToDuration(table.getPartitionUnit, table.getPartitionSize)
  } else {
    TimeHelper.convertToDuration(PartitionUnit.MINUTES, 1)
  }
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Single invocation of usercode (possibly bulk-modus)
   * (Relies on the dependencyResults for getting the dependencies and on the startTS and endTSExcl to get the sources
   * and write the right partitions
   *
   * @param dependencyResults a list with for each dependency which partitions need to be loaded
   * @param partitionTS       a list of partition starting timestamps
   */
  private def singleRun(dependencyResults: List[BulkDependencyResult], partitionTS: Array[LocalDateTime]): Boolean = {
    // TODO: Log start of single task for table here
    val startTs = partitionTS.head
    val endTs = {
      val lastInclusivePartitionTs = partitionTS.last
      Option(lastInclusivePartitionTs.plus(tableDuration))
    }
    logger.info("Start of Single Run")
    logger.info(s"TS: ${startTs} Optional end-TS: ${endTs}")

    val loadedInputDepDFs: List[(Ident, DataFrame)] =
      if (dependencyResults.nonEmpty) {
        dependencyResults
          .map(bd => {
            val id = DependencyHelper.extractTableIdentity(bd)
            val df = frameManager.loadDependencyDataFrame(bd)

            id -> df
          })
        //val df: DataFrame = null
      } else {
        Nil
      }

    val sources = table.getSources
    val loadedInputSourceDFs: List[(Ident, DataFrame)] =
      if ((sources != null) && sources.nonEmpty) {
        sources
          .filter(s => s.isAutoloading)
          .map(s => {
            val df = if (table.isPartitioned) {
              frameManager.loadSourceDataFrame(s, Option(startTs), endTs)
            } else {
              frameManager.loadSourceDataFrame(s)
            }
            val id = SourceHelper.extractSourceIdent(s)
            id -> df
          })
          .toList
      } else {
        Nil
      }

    val notLoadedInputSources: List[(SourceIdent, DependentLoaderSource)] =
      if ((sources != null) && sources.nonEmpty) {
        sources
          .filter(s => !s.isAutoloading)
          .map(s => {
            val dL = if (table.isPartitioned) {
              DependentLoaderSource(s, frameManager, Option(startTs), endTs)
            } else {
              DependentLoaderSource(s, frameManager)
            }
            val id = SourceHelper.extractSourceIdent(s)
            id -> dL
          })
          .toList
      } else {
        Nil
      }

    val inputMapLoaded = (loadedInputDepDFs ::: loadedInputSourceDFs).toMap
    val taskInput = TaskInput(inputMapLoaded, notLoadedInputSources.toMap)

    val success =
      try {
        val taskOutput = userFunc(taskInput)
        val frame = taskOutput.frame
        // TODO error checking: if target should be on datalake but no frame is given
        // Note: We are responsible for all standard writing of DataFrames
        if (!frame.isEmpty) {
          frameManager.writeDataFrame(frame, table, partitionTS, taskOutput.dataFrameWriterOptions)
        }
        true
      } catch {
        case e: Throwable => {
          logger.error("Task failed: " + startTs.toString)
          e.printStackTrace()
          false
        }
      }
    logger.info("End of Single Run")
    // TODO: Proper logging here
    success
  }

  /**
   * Process table for given array of partition (start) datetimes
   *
   * @param partitionsTs Array of localdatetime denoting the starttimes of the partitions
   * @param ex           execution context onto which the futures are created
   * @return list of futures for the started tasks
   */
  def runTasks(partitionsTs: Array[LocalDateTime], overwrite: Boolean = false)(implicit
                                                                               ex: ExecutionContext
  ): List[Future[Boolean]] = {
    val dependencyRes: TablePartitionResultSet =
      metastore.tableDependencyService.processingPartitions(table, partitionsTs)

    if (dependencyRes.getFailedTs != null) {
      logger.info(s"Failed partitionTs: ${dependencyRes.getFailedTs.mkString(", ")}")
    }

    // If no tasks to be done => quit
    val taskTimes = dependencyRes.getResolvedTs
    if ((taskTimes == null) || taskTimes.isEmpty) {
      logger.info("No tasks ready to run")
      return Nil
    }

    // TODO: Reporting of Missing LocalDateTime?

    val groups: List[Array[TablePartitionResult]] =
      DependencyHelper.createTablePartitionResultGroups(dependencyRes, tableDuration, table.getMaxBulkSize)
    val tasks: List[Future[Boolean]] = groups.map(partitionGroup => {
      val localGroupTs: Array[LocalDateTime] = partitionGroup.map(_.getPartitionTs)
      val bulkInput: List[BulkDependencyResult] =
        DependencyHelper.extractBulkDependencyResult(partitionGroup)
      logger.info(s"BulkInput length: ${bulkInput.length}")
      Future {
        val res = singleRun(bulkInput, localGroupTs)
        if (res) {
          table.getTargets.foreach((t: Target) => {
            val partitions =
              TableWrapper.createPartitions(localGroupTs, table.isPartitioned, table.getPartitionUnit, table.getPartitionSize, t.getId)
            val _ = metastore.partitionService.save(partitions, overwrite)
            partitions
              .zip(partitionGroup)
              .map(x =>
                metastore.partitionDependencyService.saveAll(x._1.getId, x._2.getResolvedDependencies, overwrite))
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
   *
   * @param startTimes sequence of partitionTS which need to be processed
   * @param threads    number of threads used by the threadpool
   */
  def runTasksAndWait(
      startTimes: Array[LocalDateTime] = Array(),
      overwrite: Boolean = false,
      threads: Option[Int] = Option.empty
  ): List[Boolean] = {
    val startTimesAdjusted = if (startTimes.isEmpty) {
      if (!table.isPartitioned) {
        Array(LocalDateTime.now())
      } else {
        throw new IllegalArgumentException("For partitioned tables the startTimes need to be specified.")
      }
    } else {
      if (!table.isPartitioned) {
        logger.info(
          "Specifying startTimes for unpartitioned tables is not necessary, " +
            "it will always run with the current timestamp."
        )
        Array(LocalDateTime.now())
      } else {
        startTimes
      }
    }

    val executor = if (threads.isEmpty) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads.get)
    }
    implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(executor)
    val futures = runTasks(startTimesAdjusted, overwrite)
    val result = Await.result(Future.sequence(futures), duration.Duration(24, duration.HOURS))
    executor.shutdown()
    result
  }

  /**
   * Get timestamp of latest processed partition datetime.
   * In case there was no partition returns empty
   * In case there are multiple targets it returns the earliest of all the targets their latest partition
   * @return localdatetime of the partition or empty if there is no
   */
  def getTimeLatestPartition(): Option[LocalDateTime] = {
    val targets = metastore.targetService.getTableTargets(wrappedTable.getId)
    val latestPartitions = targets.map(t => metastore.partitionService.getLatestPartition(t.getId)).filter(_ != null)
    if (latestPartitions.isEmpty) {
      Option.empty
    } else {
      val earliestLatestPartition = latestPartitions.reduce((partA, partB) => {
        if (partA.getPartitionTs.isAfter(partB.getPartitionTs)) {
          partB
        } else {
          partA
        }
      })
      Option(earliestLatestPartition.getPartitionTs)
    }
  }
}
