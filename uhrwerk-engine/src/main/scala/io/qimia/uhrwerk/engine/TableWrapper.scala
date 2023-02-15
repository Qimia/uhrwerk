package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.common.metastore.dependency.{
  TablePartitionResult,
  TablePartitionResultSet
}
import io.qimia.uhrwerk.common.metastore.model.{
  Partition,
  PartitionUnit,
  TableModel
}
import io.qimia.uhrwerk.common.utils.TemplateUtils
import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent}
import io.qimia.uhrwerk.engine.TableWrapper.reportProcessingPartitions
import io.qimia.uhrwerk.engine.tools.{
  DependencyHelper,
  SourceHelper,
  TimeHelper
}
import io.qimia.uhrwerk.framemanager.SparkFrameManager
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import java.time.{Duration, LocalDateTime}
import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent._

object TableWrapper {

  /** Construct an array of partitions for a particular target
    * @param partitions for which datetimes to create partitions (startTime of partition)
    * @param partitioned set to false if partitions belong to a "snapshot table"
    * @param partitionUnit partition-duration: for which length of time
    * @param partitionSize partition-duration: how many times that length of time
    * @param targetId id of target (to which the partitions belong)
    * @return Array of partition objects
    */
  def createPartitions(
      partitions: Seq[LocalDateTime],
      partitioned: Boolean,
      partitionUnit: PartitionUnit,
      partitionSize: Int,
      targetKey: Long,
      tableKey: Long,
      partitionValues: Map[String, Any] = Map.empty,
      paritionPath: String = null
  ): Seq[Partition] = {
    partitions.map(t => {
      val newPart = new Partition()
      newPart.setTargetKey(targetKey)
      newPart.setTableKey(tableKey)
      newPart.setPartitionTs(t)
      newPart.setPartitioned(partitioned)
      newPart.setPartitionSize(partitionSize)
      newPart.setPartitionUnit(partitionUnit)
      newPart.setPartitionValues(partitionValues.asJava)
      newPart.setPartitionPath(paritionPath)
      newPart
    })
  }

  /** report the result of processingPartitions to the user through the logger
    * @param result return object from TableDependencyService -> processingPartitions
    */
  def reportProcessingPartitions(
      result: TablePartitionResultSet,
      logger: Logger
  ): Unit = {
    if ((result.getFailedTs != null) && result.getFailedTs.nonEmpty) {
      result.getFailed.foreach(failedPartitionResult => {
        val failedDepNames = failedPartitionResult.getFailedDependencies
          .map(failedDep => failedDep.getDependency)
          .map(dep => dep.getTableName)
          .mkString(", ")
        logger.info(
          s"Failed partition TS: ${failedPartitionResult.getPartitionTs} - missing: $failedDepNames"
        )
      })
    }

    val processedTs = result.getProcessedTs
    if ((processedTs != null) && processedTs.nonEmpty) {
      logger.info(
        s"Previously processed partitionTs: ${processedTs.mkString(", ")}"
      )
    }

    val resolvedTs = result.getResolvedTs
    if ((resolvedTs != null) && resolvedTs.nonEmpty) {
      logger.info(s"Ready to run partitionTs: ${resolvedTs.mkString(", ")}")
    }
  }
}

class TableWrapper(
    metastore: MetaStore,
    table: TableModel,
    userFunc: TaskInput => TaskOutput,
    frameManager: FrameManager,
    properties: mutable.Map[String, AnyRef] = mutable.HashMap()
) {
  val wrappedTable: TableModel = table
  val tableDuration: Duration = if (table.getPartitioned) {
    TimeHelper.convertToDuration(table.getPartitionUnit, table.getPartitionSize)
  } else {
    TimeHelper.convertToDuration(PartitionUnit.MINUTES, 1)
  }
  private val logger: Logger = Logger.getLogger(this.getClass)

  /** Single invocation of user source-code (possibly bulk-modus)
    * (Relies on the dependencyResults for getting the dependencies and on the startTS and endTSExcl to get the sources
    * and write the right partitions
    *
    * @param dependencyResults a list with for each dependency which partitions need to be loaded
    * @param partitionTS       a list of partition starting timestamps
    */
  private def singleRun(
      dependencyResults: List[BulkDependencyResult],
      partitionTS: Array[LocalDateTime]
  ): Option[List[Map[String, Any]]] = {
    // TODO: Log start of single task for table here
    val startTs = partitionTS.head
    val endTs = {
      val lastInclusivePartitionTs = partitionTS.last
      Option(lastInclusivePartitionTs.plus(tableDuration))
    }
    logger.info(
      s"${table.getName}: Start single run TS: $startTs Optional end-TS: $endTs"
    )

    val loadedInputDepDFs: List[(Ident, DataFrame)] =
      if (dependencyResults.nonEmpty) {
        dependencyResults
          .map(bd => {
            val id = DependencyHelper.extractTableIdentity(bd)
            val df = frameManager.loadDependencyDataFrame(bd)

            if (
              bd.dependency.getViewName != null
              && !bd.dependency.getViewName.isEmpty
            )
              df.createOrReplaceTempView(bd.dependency.getViewName)

            if (df.isEmpty) {
              logger.warn(
                s"${table.getName}: - $id doesn't have any data in DataFrame"
              )
            }
            id -> df
          })
      } else {
        Nil
      }

    val sources = table.getSources
    val loadedInputSourceDFs: List[(Ident, DataFrame)] =
      if ((sources != null) && sources.nonEmpty) {
        sources
          .filter(s => s.getAutoLoad)
          .map(s => {
            val df = if (table.getPartitioned) {
              frameManager.loadSourceDataFrame(s, Option(startTs), endTs)
            } else {
              frameManager.loadSourceDataFrame(s, properties = this.properties)
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
          .filter(s => !s.getAutoLoad)
          .map(s => {
            val dL = if (table.getPartitioned) {
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

    var frame: DataFrame = null

    val success =
      try {

        var taskOutput: TaskOutput = null

        if (
          table.getTransformSqlQuery != null
          && !table.getTransformSqlQuery.isEmpty
        ) {

          val propValues = table.getTableVariables
            .flatMap(vr => {
              val opt = this.properties.get(vr)
              if (opt.isDefined) {
                Some((vr, opt.get))
              } else
                None
            })
            .toMap

          val renderedQuery = TemplateUtils.renderTemplate(
            table.getTransformSqlQuery,
            propValues.asJava
          )

          frame = frameManager
            .asInstanceOf[SparkFrameManager]
            .getSpark
            .sql(renderedQuery)

          taskOutput = TaskOutput(frame)
        } else {
          taskOutput = userFunc(taskInput)
          frame = taskOutput.frame
        }

        //TODO: cache frame after sql query is applied
        frame.cache()

        if (
          !frame.isEmpty &&
          table.getPartitionColumns != null &&
          table.getPartitionColumns.nonEmpty
        ) {
          val notInFramePartCols = table.getPartitionColumns.filter(partCol =>
            frame.columns.find(_.equalsIgnoreCase(partCol.trim)).isEmpty
          )
          if (notInFramePartCols.nonEmpty)
            for (partCol <- notInFramePartCols) {
              val opt = this.properties.get(partCol.trim)
              if (opt.isDefined)
                frame = frame.withColumn(partCol.trim, lit(opt.get))
              else
                logger.error(
                  s"Partition column $partCol is not in the frame and no property with the same name is given"
                )
            }
          taskOutput = TaskOutput(
            frame,
            Some(
              Array(
                Map(
                  "partitionBy" -> table.getPartitionColumns
                    .map(_.trim)
                    .mkString(",")
                )
              )
            )
          )

        }

        // TODO error checking: if target should be on datalake but no frame is given
        // Note: We are responsible for all standard writing of DataFrames
        if (!frame.isEmpty) {
          frameManager.writeDataFrame(
            frame,
            table,
            partitionTS,
            taskOutput.dataFrameWriterOptions
          )

          if (
            table.getPartitionColumns != null &&
            table.getPartitionColumns.nonEmpty
          ) {
            val partCols = table.getPartitionColumns.map(_.trim)

            val groupedDf =
              if (partCols.length > 1)
                frame
                  .groupBy(partCols.head, partCols.tail: _*)
              else
                frame.groupBy(partCols.head)

            val cols = partCols.toSeq
            val partValues = groupedDf
              .count()
              .drop("count")
              .collect()
              .map(row => row.getValuesMap[Any](cols))
              .toList
            Some(partValues)
          } else {
            Some(List.empty)
          }
        } else {
          logger.warn(s"No output for ${table.getName} (!!)")
          None
        }
      } catch {
        case e: Throwable =>
          logger.error(s"${table.getName}: Task failed: " + startTs.toString)
          e.printStackTrace()
          None
      } finally {
        if (frame != null) {
          frame.unpersist(blocking = false)
        }
      }
    logger.info(s"${table.getName}: Single run done, success = $success")
    // TODO: Proper logging here
    success
  }

  /** Process table for given array of partition (start) datetimes
    *
    * @param partitionsTs Array of localdatetime denoting the starttimes of the partitions
    * @param ex           execution context onto which the futures are created
    * @return list of futures for the started tasks
    */
  def runTasks(
      partitionsTs: List[LocalDateTime],
      overwrite: Boolean = false
  )(implicit
      ex: ExecutionContext
  ): List[Future[Boolean]] = {
    val tasks = getTaskRunners(partitionsTs, overwrite).map(x =>
      Future {
        x()
      }
    )

    /** Idea: Give a complete report at the end after calling runTasks
      * Showing partitions already there, partitions with missing dependencies
      * and if anything failed or went wrong while producing certain partitions
      * (this also means giving the user more info there)
      */
    tasks
  }

  /** Process table for given array of partition (start) datetimes
    *
    * @param partitionsTs Array of localdatetime denoting the starttimes of the partitions
    * @param ex           execution context onto which the futures are created
    * @return list of futures for the started tasks
    */
  def getTaskRunners(
      partitionsTs: List[LocalDateTime],
      overwrite: Boolean = false
  )(implicit ex: ExecutionContext): immutable.List[() => Boolean] = {
    val dependencyRes: TablePartitionResultSet =
      metastore.tableDependencyService.processingPartitions(
        table,
        partitionsTs.asJava,
        this.properties.asJava
      )

    reportProcessingPartitions(dependencyRes, logger)

    // If no tasks to be done => quit
    val taskTimes = dependencyRes.getResolvedTs
    if ((taskTimes == null) || taskTimes.isEmpty) {
      logger.info("No tasks ready to run")
      return Nil
    }

    // TODO: Reporting of Missing LocalDateTime?

    val groups: List[Array[TablePartitionResult]] =
      DependencyHelper.createTablePartitionResultGroups(
        dependencyRes,
        tableDuration,
        table.getMaxBulkSize
      )
    val tasks: List[() => Boolean] = groups.map(partitionGroup => {
      val localGroupTs: Array[LocalDateTime] =
        partitionGroup.map(_.getPartitionTs)
      val bulkInput: List[BulkDependencyResult] =
        DependencyHelper.extractBulkDependencyResult(partitionGroup)
      logger.info(s"Bulk input length: ${bulkInput.length}")

      val e = () => {
        val res = singleRun(bulkInput, localGroupTs)
        if (res.isDefined) {
          var partitions: List[Partition] = Nil

          if (res.get.isEmpty) {
            partitions = this.table.getTargets
              .flatMap(target => {
                TableWrapper.createPartitions(
                  localGroupTs,
                  table.getPartitioned,
                  table.getPartitionUnit,
                  table.getPartitionSize,
                  target.getHashKey,
                  target.getTableKey
                )
              })
              .toList
          } else {
            partitions = res.get.flatMap(partValues => {
              val partPath = table.getPartitionColumns
                .map(partCol => {
                  val colValue = partValues.get(partCol).get
                  s"$partCol=$colValue"
                })
                .mkString("/")
              table.getTargets.flatMap(target => {
                TableWrapper.createPartitions(
                  localGroupTs,
                  table.getPartitioned,
                  table.getPartitionUnit,
                  table.getPartitionSize,
                  target.getHashKey,
                  target.getTableKey,
                  partValues,
                  partPath
                )
              })
            })
          }
          logger.info(s"Saving ${partitions.length} partitions")
          for (partition <- partitions) {
            logger.info(s"Saving partition $partition")
          }

          val results =
            metastore.partitionService.save(partitions.asJava, overwrite)

          logger.info(
            s"Saved ${results.asScala.filter(_.isSuccess).size} partitions successfully"
          )
          results.asScala
            .filter(_.isSuccess)
            .foreach(partition => {
              logger.error(s"Saved Partition: $partition")
            })

          partitions
            .zip(partitionGroup)
            .map(x =>
              metastore.partitionDependencyService
                .saveAll(
                  x._1.getId,
                  x._2.getResolvedDependencies,
                  overwrite
                )
            )
          // TODO: Need to handle failure to store
        }

        res.isDefined
      }
      e
    })
    tasks
  }

  /** Idea: Give a complete report at the end after calling runTasks
    * Showing partitions already there, partitions with missing dependencies
    * and if anything failed or went wrong while producing certain partitions
    * (this also means giving the user more info there)
    */

  /** Utility function to create an execution context and block until runTasks is done processing the batches.
    * See [[io.qimia.uhrwerk.engine.TableWrapper#runTasks]]
    *
    * @param startTimes sequence of partitionTS which need to be processed
    * @param threads    number of threads used by the threadpool
    */
  def runTasksAndWait(
      startTimes: List[LocalDateTime] = List(),
      overwrite: Boolean = false,
      threads: Option[Int] = Option.empty
  ): List[Boolean] = {
    val startTimesAdjusted = if (startTimes.isEmpty) {
      if (!table.getPartitioned) {
        List(LocalDateTime.now())
      } else {
        throw new IllegalArgumentException(
          "For partitioned tables the startTimes need to be specified."
        )
      }
    } else {
      if (!table.getPartitioned) {
        logger.info(
          "Specifying startTimes for unpartitioned tables is not necessary, " +
            "it will always run with the current timestamp."
        )
        List(LocalDateTime.now())
      } else {
        startTimes
      }
    }

    val executor = if (threads.isEmpty) {
      Executors.newSingleThreadExecutor()
    } else {
      Executors.newFixedThreadPool(threads.get)
    }
    implicit val executionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(executor)
    val futures = runTasks(startTimesAdjusted, overwrite)
    val result = Await.result(
      Future.sequence(futures),
      duration.Duration(24, duration.HOURS)
    )
    executor.shutdown()
    result
  }

  override def toString = s"TableWrapper($wrappedTable)"

  override def equals(other: Any) = this.wrappedTable.equals(other)

  override def hashCode() = wrappedTable.hashCode()
}
