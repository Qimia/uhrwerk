package io.qimia.uhrwerk.framemanager

import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.common.metastore.model.{
  ConnectionType,
  IngestionMode,
  SourceModel2,
  TableModel
}
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.common.tools.{JDBCTools, TimeTools}
import io.qimia.uhrwerk.common.utils.TemplateUtils
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils._
import org.apache.commons.lang.RandomStringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.Properties
import scala.collection.JavaConverters._

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def getSpark = this.sparkSession

  /** Loads a source dataframe. Either one batch or the full path.
    *
    * @param source                 Source
    * @param startTS                Batch Timestamp. If not defined, load the full path.
    * @param endTSExcl              End Timestamp exclusive
    * @param dataFrameReaderOptions Optional Spark reading options.
    * @return DataFrame with the uhrwerk time columns added.
    * @throws IllegalArgumentException In case one or both timestamps are specified but the source select column is empty
    */
  override def loadSourceDataFrame(
      source: SourceModel2,
      startTS: Option[LocalDateTime] = Option.empty,
      endTSExcl: Option[LocalDateTime] = Option.empty,
      dataFrameReaderOptions: Option[Map[String, String]] = Option.empty,
      properties: Properties = new Properties()
  ): DataFrame = {
    if (
      (startTS.isDefined || endTSExcl.isDefined) &&
      source.getIngestionMode == IngestionMode.INTERVAL &&
      isStringEmpty(source.getIntervalColumn)
    ) {
      throw new IllegalArgumentException(
        "When one or both of the timestamps are specified, " +
          "the source.selectColumn needs to be set as well."
      )
    }

    if (source.getConnection.getType.equals(ConnectionType.JDBC)) {
      loadSourceFromJDBC(
        source,
        startTS,
        endTSExcl,
        dataFrameReaderOptions,
        properties
      )
    } else {
      loadDataFrameFromFileSystem(
        source,
        startTS,
        endTSExcl,
        dataFrameReaderOptions
      )
    }
  }

  /** Loads a dataframe from a file system.
    *
    * @param source                 Source
    * @param startTS                Batch Timestamp. If not defined, load the full path.
    * @param endTSExcl              End Timestamp exclusive
    * @param dataFrameReaderOptions Optional Spark reading options.
    * @return Loaded DataFrame.
    */
  private def loadDataFrameFromFileSystem(
      source: SourceModel2,
      startTS: Option[LocalDateTime],
      endTSExcl: Option[LocalDateTime],
      dataFrameReaderOptions: Option[Map[String, String]]
  ): DataFrame = {
    val fullLocation =
      getFullLocation(source.getConnection.getPath, source.getPath)

    val reader = if (dataFrameReaderOptions.isDefined) {
      sparkSession.read
        .options(dataFrameReaderOptions.get)
    } else {
      sparkSession.read
    }

    val df = reader.format(source.getFormat).load(fullLocation.head)

    val filteredDf: DataFrame = if (startTS.isDefined && endTSExcl.isDefined) {
      df.filter(
        col(source.getIntervalColumn) >= TimeTools
          .convertTSToString(startTS.get)
      ).filter(
        col(source.getIntervalColumn) < TimeTools
          .convertTSToString(endTSExcl.get)
      )
    } else if (startTS.isDefined) {
      val res = df.filter(
        col(source.getIntervalColumn) >= TimeTools
          .convertTSToString(startTS.get)
      )
      res.show(10)
      res
    } else if (endTSExcl.isDefined) {
      df.filter(
        col(source.getIntervalColumn) < TimeTools
          .convertTSToString(endTSExcl.get)
      )
    } else {
      df
    }

    if (
      !isStringEmpty(source.getIntervalColumn) && !containsTimeColumns(
        filteredDf,
        source.getIntervalTempUnit
      )
    ) {
      addTimeColumnsToDFFromTimestampColumn(
        filteredDf,
        source.getIntervalColumn
      )
    } else {
      filteredDf
    }
  }

  /** Loads all partitions from the succeeded partitions of the DependencyResult.
    *
    * @param dependencyResult       DependencyResult.
    * @param dataFrameReaderOptions Optional Spark reading options.
    * @return DataFrame.
    */
  override def loadDependencyDataFrame(
      dependencyResult: BulkDependencyResult,
      dataFrameReaderOptions: Option[Map[String, String]] = Option.empty
  ): DataFrame = {

    assert(dependencyResult.succeeded.nonEmpty)

    val isJDBC = dependencyResult.connection.getType.equals(ConnectionType.JDBC)

    if (dependencyResult.connection.getType.equals(ConnectionType.S3)) {
      SparkFrameManagerUtils.setS3Config(
        sparkSession,
        dependencyResult.connection
      )
    }

    val dfReader = sparkSession.read
      .format(dependencyResult.dependency.getFormat)

    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val dependencyPath =
      getDependencyPath(dependencyResult.dependency, fileSystem = !isJDBC)

    val df = {
      try {
        if (isJDBC) {
          dfReaderWithUserOptions
            .option("url", dependencyResult.connection.getJdbcUrl)
            .option("driver", dependencyResult.connection.getJdbcDriver)
            .option("user", dependencyResult.connection.getJdbcUser)
            .option("password", dependencyResult.connection.getJdbcPass)
            .option(
              "dbtable",
              dependencyPath
            )
            .load()
        } else {

          val partPaths: List[String] = dependencyResult.succeeded
            .map(_.getPartitionPath)
            .filter(path => path != null && path.nonEmpty)
            .toList

          val tmpDf = dfReaderWithUserOptions
            .load(
              getFullLocation(
                dependencyResult.connection.getPath,
                dependencyPath
              ).head
            )
          val partFilters = dependencyResult.succeeded
            .filter(_.getPartitionValues != null)
            .map(_.getPartitionValues.asScala)
            .flatten
            .groupBy(_._1)
            .mapValues(_.map(_._2))
          if (partFilters.nonEmpty)
            tmpDf.filter(
              partFilters
                .map { case (k, v) => col(k).isin(v: _*) }
                .reduce(_ && _)
            )
          else
            tmpDf
        }
      } catch {
        case exception: Exception =>
          throw new Exception(
            s"Something went wrong with reading of the DataFrame for dependency: $dependencyPath" +
              "\nThe DataFrame was probably saved in previous steps with empty partitions only." +
              "\nAs the Metastore has some partition information for this dependency (otherwise this job wouldn't have run), " +
              "that table was saved with empty partitions." +
              "\nThis is made by design, when e.g. 1 of out 100 partitions is empty, all 100 are " +
              "marked as successfully processed. " +
              "\nIn a job with a dependency on that empty partition an empty DataFrame with a proper schema is returned." +
              "\nExcept when all saved partitions were empty, then the table doesn't yet exist on the datalake (database).",
            exception
          )
      }
    }

    val filtered = df

    val always = true
    if (always) {
      // unpartitioned data, remove all time columns
      filtered
        .drop(timeColumnJDBC)
        .drop(timeColumns: _*)
    } else {
      // partitioned data
      if (isJDBC) {
        addTimeColumnsToDFFromTimestampColumn(
          filtered,
          timeColumnJDBC
        ).drop(timeColumnJDBC)
      } else {
        filtered
      }
    }
  }

  /** Loads a dataframe from JDBC. Either one batch or the full path.
    * If the selectQuery is set, it uses that instead of the path.
    * If the partitionQuery is set, it first runs this to get the partition boundaries.
    *
    * @param source                 Source
    * @param startTS                Batch Timestamp. If not defined, load the full path.
    * @param endTSExcl              End Timestamp exclusive
    * @param dataFrameReaderOptions Optional Spark reading options.
    * @return DataFrame
    */
  private def loadSourceFromJDBC(
      source: SourceModel2,
      startTS: Option[LocalDateTime],
      endTSExcl: Option[LocalDateTime],
      dataFrameReaderOptions: Option[Map[String, String]],
      properties: Properties = new Properties()
  ): DataFrame = {

    val dfReader: DataFrameReader =
      JDBCTools.getDbConfig(sparkSession, source.getConnection)
    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    dfReaderWithUserOptions.option("fetchsize", 100000)

    val dfReaderWithQuery: DataFrameReader =
      if (!isStringEmpty(source.getSelectQuery)) {

        var selectQuery = source.getSelectQuery

        if (
          source.getSourceVariables != null && source.getSourceVariables.nonEmpty
        ) {
          val propValues = source.getSourceVariables
            .flatMap(vr => {
              val propVal = properties.getProperty(vr)
              if (propVal != null && propVal.nonEmpty) {
                Some((vr, propVal))
              } else
                None
            })
            .toMap
          selectQuery = TemplateUtils.renderTemplate(
            selectQuery,
            propValues.asJava
          )
        }
        val query: String =
          JDBCTools.createSelectQuery(
            source.getSelectQuery,
            startTS,
            endTSExcl,
            Option(
              getFullLocationJDBC(source.getConnection.getPath, source.getPath)
            )
          )

        val dfReaderWithQuery = dfReaderWithUserOptions
          .option("dbtable", query)
          .option(
            "numPartitions",
            source.getParallelPartitionNum.toString
          )

        if (!isStringEmpty(source.getParallelPartitionQuery)) {
          var parallelPartitionQuery = source.getParallelPartitionQuery
          if (
            source.getSourceVariables != null && source.getSourceVariables.nonEmpty
          ) {
            val propValues = source.getSourceVariables
              .flatMap(vr => {
                val propVal = properties.getProperty(vr)
                if (propVal != null && propVal.nonEmpty) {
                  Some((vr, propVal))
                } else
                  None
              })
              .toMap
            parallelPartitionQuery = TemplateUtils.renderTemplate(
              parallelPartitionQuery,
              propValues.asJava
            )
          }

          val (minId, maxId) = JDBCTools.minMaxQueryIds(
            sparkSession,
            source.getConnection,
            source.getParallelPartitionColumn,
            parallelPartitionQuery,
            startTS,
            endTSExcl,
            Option(
              getFullLocationJDBC(source.getConnection.getPath, source.getPath)
            )
          )

          minId match {
            case _: Long =>
              dfReaderWithQuery
                .option("partitionColumn", source.getParallelPartitionColumn)
                .option("lowerBound", minId.asInstanceOf[Long])
                .option("upperBound", maxId.asInstanceOf[Long])
            case _: Timestamp =>
              dfReaderWithQuery
                .option("partitionColumn", source.getParallelPartitionColumn)
                .option("lowerBound", minId.asInstanceOf[Timestamp].toString)
                .option("upperBound", maxId.asInstanceOf[Timestamp].toString)
          }
        }
        dfReaderWithQuery
      } else {
        dfReader
          .option("dbtable", source.getPath) // area-vertical.tableName-version
      }

    logger.info(s"Loading source ${source.getPath}")

    val df: DataFrame = dfReaderWithQuery
      .load()

    if (
      !isStringEmpty(source.getIntervalColumn) && !containsTimeColumns(
        df,
        source.getIntervalTempUnit
      )
    ) {
      addTimeColumnsToDFFromTimestampColumn(df, source.getIntervalColumn)
    } else {
      df
    }
  }

  /** Saves a table to all its targets.
    * Four possible scenarios regarding the timestamp:
    * 1. Time columns (these five: year, month, day, hour, minute) are in the DF
    * 1.1 Identity transformation (partitionSize = 1)
    * 1.1.1 File system => columns are normally used for partitioning
    * 1.1.2 JDBC => one time stamp column is created from the time columns and that is then used for saving
    * 1.2 Not identity => a new timestamp column is created based on the partitionTS so that it fits into the aggregate
    * 1.2.1 File system => time columns are created from the timestamp column
    * 1.2.2 JDBC => the DF is saved used the timestamp column
    * 2. Time columns are missing in the DF and partitionTS contains at least one item => time columns are created.
    * This can mean that the table is also unpartitioned but this makes no difference for saving the data.
    * 3. partitionTS is empty and time columns are not in the DF => no partitioning is used
    *
    * @param frame                  DataFrame to save.
    * @param table                  Table information.
    * @param partitionTS            An array of timestamps (partitions).
    * @param dataFrameWriterOptions Optional array of Spark writing options.
    *                               If the array has only one item (one map), this one is used for all targets.
    *                               If the array has as many items as there are targets,
    *                               each target is saved with different options.
    *                               The options need to be valid Spark options with one exception: partitionBy.
    *                               This is a comma-separated list of columns that should be used for partitioning
    *                               (extra from the uhrwerk timestamp columns).
    */
  override def writeDataFrame(
      frame: DataFrame,
      table: TableModel,
      partitionTS: Array[LocalDateTime],
      dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty
  ): Unit = {

    table.getTargets.zipWithIndex.foreach((item: (TargetModel, Int)) => {
      val target: TargetModel = item._1
      val index: Int = item._2
      val targetConnection = target.getConnection
      if (targetConnection == null) {
        throw new IllegalArgumentException(
          "A connection is missing in the target."
        )
      }
      val isJDBC = targetConnection.getType.equals(ConnectionType.JDBC)

      if (targetConnection.getType == ConnectionType.S3) {
        SparkFrameManagerUtils.setS3Config(sparkSession, targetConnection)
      }

      val isRedshift = targetConnection.getType.equals(ConnectionType.REDSHIFT)

      val tablePath = getTablePath(table, !isJDBC, target.getFormat)
      val path = if (isJDBC || isRedshift) {
        tablePath
      } else {
        getFullLocation(targetConnection.getPath, tablePath).head
      }

      val dfContainsTimeColumns =
        containsTimeColumns(frame, table.getPartitionUnit)

      val (fullPath, df) =
        if (
          !dfContainsTimeColumns && partitionTS.nonEmpty && table.getPartitioned
        ) {
          // if time columns are missing, saving just one partition defined in the partitionTS array

          // for jdbc add a timestamp column and remove all other time columns (year/month/day/hour/minute) just in case
          if (isJDBC || isRedshift) {
            val jdbcDF =
              addJDBCTimeColumn(frame, partitionTS.head).drop(timeColumns: _*)

            (path, jdbcDF)
            // for fs remove all time columns (year/month/day/hour/minute)
          } else {
            val datePath = createDatePath(partitionTS.head)

            (concatenatePaths(path, datePath), frame.drop(timeColumns: _*))
          }
        } else if (dfContainsTimeColumns) {
          // if it is not identity, need to rewrite the values in the time columns
          if (table.getPartitionSize > 1) {
            val partitionUnit = table.getPartitionUnit.toString
            val partitionSize = table.getPartitionSize
            val dfWithNewTimeColumnsTmp =
              addJDBCTimeColumnFromTimeColumns(frame)
                .drop(timeColumns: _*)
                .withColumn(
                  timeColumnJDBC,
                  udf { x: Timestamp =>
                    Timestamp.valueOf(
                      TimeTools.getAggregateForTimestamp(
                        partitionTS,
                        x.toLocalDateTime,
                        partitionUnit,
                        partitionSize
                      )
                    )
                  }.apply(col(timeColumnJDBC))
                )

            if (isJDBC || isRedshift) {
              (path, dfWithNewTimeColumnsTmp)
            } else {
              (
                path,
                addTimeColumnsToDFFromTimestampColumn(
                  dfWithNewTimeColumnsTmp,
                  timeColumnJDBC
                ).drop(timeColumnJDBC)
              )
            }
          } else {
            // if jdbc and saving several partitions (the df contains the time columns) - add the timestamp column
            // and remove the time columns
            if (isJDBC || isRedshift) {
              (
                path,
                addJDBCTimeColumnFromTimeColumns(frame).drop(timeColumns: _*)
              )
            } else {
              (path, frame)
            }
          }
        } else {
          // no time columns, nothing in partitionTS
          (path, frame)
        }

      val writer = df.write.mode(SaveMode.Overwrite).format(target.getFormat)
      val (writerWithOptions: DataFrameWriter[Row], partitionBy: List[String]) =
        if (dataFrameWriterOptions.isDefined) {
          val optionsTmp =
            if (dataFrameWriterOptions.get.length == table.getTargets.length) {
              dataFrameWriterOptions.get(index)
            } else {
              dataFrameWriterOptions.get.head
            }
          val (options, partitionBy: List[String]) =
            if (optionsTmp.contains("partitionBy")) {
              (
                optionsTmp.filterNot(p => p._1 == "partitionBy"),
                optionsTmp
                  .find(p => p._1 == "partitionBy")
                  .get
                  ._2
                  .split(",")
                  .map(_.trim)
                  .toList
              )
            } else {
              (optionsTmp, List[String]())
            }
          (
            writer
              .options(options),
            partitionBy
          )
        } else {
          (writer, List[String]())
        }

      val writerWithPartitioning =
        if (!isJDBC && !isRedshift && dfContainsTimeColumns) {
          writerWithOptions
            .partitionBy(timeColumns ++ partitionBy: _*)
        } else {
          writerWithOptions
        }

      logger.info(s"Saving DF to $fullPath")
      if (isJDBC) {
        val dbTableName =
          if (
            target.getTableName != null
            && !target.getTableName.trim.isEmpty
          )
            target.getTableName.trim
          else
            fullPath

        val jdbcWriter = writerWithPartitioning
          .option("url", targetConnection.getJdbcUrl)
          .option("driver", targetConnection.getJdbcDriver)
          .option("user", targetConnection.getJdbcUser)
          .option("password", targetConnection.getJdbcPass)
          .option("dbtable", s"$dbTableName")
        try {
          jdbcWriter
            .save()
        } catch {
          case e: Exception =>
            logger.warn(e.getLocalizedMessage)
            logger.warn("Trying to create the database")
            JDBCTools.createJDBCDatabase(
              targetConnection,
              fullPath.split("\\.")(0)
            )
            jdbcWriter
              .save()
        }

        if (df.columns.contains(timeColumnJDBC)) {
          val (tableSchema, tableName) = getJDBCTableSchemaAndName(table)
          JDBCTools.addIndexToTable(
            targetConnection,
            tableSchema,
            tableName,
            timeColumnJDBC
          )
        }
      } else if (isRedshift) {
        val url =
          s"${targetConnection.getJdbcUrl}&user=${targetConnection.getJdbcUser}&password=${targetConnection.getJdbcPass}"
        var tmpDir = targetConnection.getRedshiftTempDir
        tmpDir =
          if (tmpDir.endsWith("/"))
            tmpDir.substring(0, targetConnection.getRedshiftTempDir.size - 1)
          else tmpDir

        val rndm = RandomStringUtils.random(8, true, true)

        var tblPath = target.getTableName
        tblPath =
          if (tblPath.contains("."))
            tblPath.replace(".", "__")
          else tblPath
        tblPath = tblPath + "__" + rndm

        writerWithPartitioning
          .format(targetConnection.getRedshiftFormat)
          .option("url", url)
          .option("dbtable", target.getTableName)
          .option("aws_iam_role", targetConnection.getRedshiftAwsIamRole)
          .option("tempdir", tmpDir + "/" + tblPath)
          .mode("append")
          .save()
      } else {
        writerWithPartitioning
          .save(fullPath)
      }
    })
  }
}
