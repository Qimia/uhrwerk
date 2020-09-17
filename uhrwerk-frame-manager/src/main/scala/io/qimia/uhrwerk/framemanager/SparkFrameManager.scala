package io.qimia.uhrwerk.framemanager

import java.sql.Timestamp
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.common.tools.{JDBCTools, TimeTools}
import io.qimia.uhrwerk.framemanager.utils.SparkFrameManagerUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, udf}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {

  /**
   * Loads a source dataframe. Either one batch or the full path.
   *
   * @param source                 Source
   * @param startTS                Batch Timestamp. If not defined, load the full path.
   * @param endTSExcl              End Timestamp exclusive
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return DataFrame with the uhrwerk time columns added.
   * @throws IllegalArgumentException In case one or both timestamps are specified but the source select column is empty
   */
  override def loadSourceDataFrame(
                                    source: Source,
                                    startTS: Option[LocalDateTime] = Option.empty,
                                    endTSExcl: Option[LocalDateTime] = Option.empty,
                                    dataFrameReaderOptions: Option[Map[String, String]] = Option.empty
                                  ): DataFrame = {
    if (
      (startTS.isDefined || endTSExcl.isDefined) && isStringEmpty(
        source.getSelectColumn
      )
    ) {
      throw new IllegalArgumentException(
        "When one or both of the timestamps are specified, " +
          "the source.selectColumn needs to be set as well."
      )
    }

    if (source.getConnection.getType.equals(ConnectionType.JDBC)) {
      loadSourceFromJDBC(source, startTS, endTSExcl, dataFrameReaderOptions)
    } else {
      loadDataFrameFromFileSystem(
        source,
        startTS,
        endTSExcl,
        dataFrameReaderOptions
      )
    }
  }

  /**
   * Loads a dataframe from a file system.
   *
   * @param source                 Source
   * @param startTS                Batch Timestamp. If not defined, load the full path.
   * @param endTSExcl              End Timestamp exclusive
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return Loaded DataFrame.
   */
  private def loadDataFrameFromFileSystem(
                                           source: Source,
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

    val df = reader.format(source.getFormat).load(fullLocation)

    val filteredDf: DataFrame = if (startTS.isDefined && endTSExcl.isDefined) {
      df.filter(
        col(source.getSelectColumn) >= TimeTools
          .convertTSToString(startTS.get)
      )
        .filter(
          col(source.getSelectColumn) < TimeTools
            .convertTSToString(endTSExcl.get)
        )
    } else if (startTS.isDefined) {
      df.filter(
        col(source.getSelectColumn) >= TimeTools
          .convertTSToString(startTS.get)
      )
    } else if (endTSExcl.isDefined) {
      df.filter(
        col(source.getSelectColumn) < TimeTools
          .convertTSToString(endTSExcl.get)
      )
    } else {
      df
    }

    if (!isStringEmpty(source.getSelectColumn) && !containsTimeColumns(filteredDf, source.getPartitionUnit)) {
      addTimeColumnsToDFFromTimestampColumn(filteredDf, source.getSelectColumn, source.getPartitionUnit)
    } else {
      filteredDf
    }
  }

  /**
   * Loads all partitions from the succeeded partitions of the DependencyResult.
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

    val filter: Column = dependencyResult.succeeded
      .map(p =>
        if (isJDBC) {
          col(timeColumnJDBC) === TimeTools.convertTSToUTCString(p.getPartitionTs)
        } else {
          val (year, month, day, hour, minute) = getTimeValues(p.getPartitionTs)
          p.getPartitionUnit match {
            case PartitionUnit.MINUTES => col("minute") === minute
            case PartitionUnit.HOURS => col("hour") === hour
            case PartitionUnit.DAYS => col("day") === day
            case _ => col("minute") === minute // unpartitioned
          }
        }
      )
      .reduce((a, b) => a || b)

    // todo speedup when full year/month/day..

    val dfReader = sparkSession.read
      .format(dependencyResult.dependency.getFormat)

    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val df =
      if (isJDBC) {
        dfReaderWithUserOptions
          .option("url", dependencyResult.connection.getJdbcUrl)
          .option("driver", dependencyResult.connection.getJdbcDriver)
          .option("user", dependencyResult.connection.getJdbcUser)
          .option("password", dependencyResult.connection.getJdbcPass)
          .option(
            "dbtable",
            getDependencyPath(dependencyResult.dependency, fileSystem = false)
          )
          .load()
      } else {
        dfReaderWithUserOptions
          .load(
            getFullLocation(
              dependencyResult.connection.getPath,
              getDependencyPath(dependencyResult.dependency, fileSystem = true)
            )
          )
      }

    val filtered = df.filter(filter)

    if (dependencyResult.dependency.getTransformType.equals(PartitionTransformType.NONE)) {
      // unpartitioned data, remove all time columns
      filtered
        .drop(timeColumnJDBC)
        .drop(timeColumns: _*)
    } else {
      // partitioned data
      if (isJDBC) {
        addTimeColumnsToDFFromTimestampColumn(
          filtered,
          timeColumnJDBC,
          dependencyResult.succeeded.head.getPartitionUnit
        ).drop(timeColumnJDBC)
      } else {
        filtered
      }
    }
  }

  /**
   * Loads a dataframe from JDBC. Either one batch or the full path.
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
                                  source: Source,
                                  startTS: Option[LocalDateTime],
                                  endTSExcl: Option[LocalDateTime],
                                  dataFrameReaderOptions: Option[Map[String, String]]
                                ): DataFrame = {

    val dfReader: DataFrameReader =
      JDBCTools.getDbConfig(sparkSession, source.getConnection)
    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val dfReaderWithQuery: DataFrameReader =
      if (!isStringEmpty(source.getSelectQuery)) {
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
            source.getParallelLoadNum
          )

        if (!isStringEmpty(source.getParallelLoadQuery)) {
          val (minId, maxId) = JDBCTools.minMaxQueryIds(
            sparkSession,
            source.getConnection,
            source.getParallelLoadColumn,
            source.getParallelLoadQuery,
            startTS,
            endTSExcl,
            Option(
              getFullLocationJDBC(source.getConnection.getPath, source.getPath)
            )
          )

          dfReaderWithQuery
            .option("partitionColumn", source.getParallelLoadColumn)
            .option("lowerBound", minId)
            .option("upperBound", maxId)
        } else if (!isStringEmpty(source.getSelectColumn) && startTS.isDefined && endTSExcl.isDefined) {
          dfReaderWithQuery
            .option("partitionColumn", source.getSelectColumn)
            .option("lowerBound", TimeTools.convertTSToUTCString(startTS.get))
            .option("upperBound", TimeTools.convertTSToUTCString(endTSExcl.get))
        } else {
          dfReaderWithQuery
        }
      } else {
        dfReader
          .option("dbtable", source.getPath) // area-vertical.tableName-version
      }

    println("Loading source")

    val df: DataFrame = dfReaderWithQuery
      .load()

    if (!isStringEmpty(source.getSelectColumn) && !containsTimeColumns(df, source.getPartitionUnit)) {
      addTimeColumnsToDFFromTimestampColumn(df, source.getSelectColumn, source.getPartitionUnit)
    } else {
      df
    }
  }

  /**
   * Saves a table to all its targets.
   * Four possible scenarios regarding the timestamp:
   * 1. Time columns (up to these five: year, month, day, hour, minute) are in the DF
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
   * @param locationTableInfo      Location Info.
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
                               locationTableInfo: Table,
                               partitionTS: Array[LocalDateTime],
                               dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty
                             ): Unit = {

    locationTableInfo.getTargets.zipWithIndex.foreach((item: (Target, Int)) => {
      val target: Target = item._1
      val index: Int = item._2
      if (target.getConnection == null) {
        throw new IllegalArgumentException(
          "A connection is missing in the target."
        )
      }
      val isJDBC = target.getConnection.getType.equals(ConnectionType.JDBC)

      val timeColumnsCut = calculateCutBasedOnPartitionUnit(locationTableInfo.getPartitionUnit)
      val selectedTimeColumns =
        timeColumns.slice(0, timeColumnsCut)

      val tablePath = getTablePath(locationTableInfo, !isJDBC, target.getFormat)
      val path = if (isJDBC) {
        tablePath
      } else {
        getFullLocation(target.getConnection.getPath, tablePath)
      }

      val dfContainsTimeColumns = containsTimeColumns(frame, locationTableInfo.getPartitionUnit)

      val (fullPath, df) = if (!dfContainsTimeColumns && !partitionTS.isEmpty) {
        // if time columns are missing, saving just one partition defined in the partitionTS array

        // for jdbc add a timestamp column and remove all other time columns (year/month/day/hour/minute)
        if (isJDBC) {
          val jdbcDF = addJDBCTimeColumn(frame, partitionTS.head).drop(selectedTimeColumns: _*)

          (path, jdbcDF)
          // for fs remove all time columns (year/month/day/hour/minute)
        } else {
          val datePath = createDatePath(partitionTS.head, locationTableInfo.getPartitionUnit)

          (concatenatePaths(path, datePath), frame.drop(selectedTimeColumns: _*))
        }
      } else if (dfContainsTimeColumns) {
        // if it is not identity, need to rewrite the values in the time columns
        if (locationTableInfo.getPartitionSize > 1) {
          val partitionUnit = locationTableInfo.getPartitionUnit.toString
          val partitionSize = locationTableInfo.getPartitionSize
          val dfWithNewTimeColumnsTmp = addJDBCTimeColumnFromTimeColumns(frame, timeColumnsCut)
            .drop(selectedTimeColumns: _*)
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

          if (isJDBC) {
            (path, dfWithNewTimeColumnsTmp)
          } else {
            (
              path,
              addTimeColumnsToDFFromTimestampColumn(
                dfWithNewTimeColumnsTmp,
                timeColumnJDBC,
                locationTableInfo.getPartitionUnit
              ).drop(timeColumnJDBC)
            )
          }
        } else {
          // if jdbc and saving several partitions (the df contains the time columns) - add the timestamp column
          // and remove the time columns
          if (isJDBC) {
            (path, addJDBCTimeColumnFromTimeColumns(frame, timeColumnsCut).drop(selectedTimeColumns: _*))
          } else {
            (path, frame)
          }
        }
      } else {
        // no time columns, nothing in partitionTS
        (path, frame)
      }

      val writer = df.write.mode(SaveMode.Append).format(target.getFormat)
      val (writerWithOptions: DataFrameWriter[Row], partitionBy: List[String]) = if (dataFrameWriterOptions.isDefined) {
        val optionsTmp =
          if (dataFrameWriterOptions.get.length == locationTableInfo.getTargets.length) {
            dataFrameWriterOptions.get(index)
          } else {
            dataFrameWriterOptions.get.head
          }
        val (options, partitionBy: List[String]) = if (optionsTmp.contains("partitionBy")) {
          (
            optionsTmp.filterNot(p => p._1 == "partitionBy"),
            optionsTmp.find(p => p._1 == "partitionBy").get._2.split(",").map(_.trim).toList
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
        if (!isJDBC && dfContainsTimeColumns) {
          writerWithOptions
            .partitionBy(selectedTimeColumns ++ partitionBy: _*)
        } else {
          writerWithOptions
        }

      println(s"Saving DF to $fullPath")
      if (isJDBC) {
        val jdbcWriter = writerWithPartitioning
          .option("url", target.getConnection.getJdbcUrl)
          .option("driver", target.getConnection.getJdbcDriver)
          .option("user", target.getConnection.getJdbcUser)
          .option("password", target.getConnection.getJdbcPass)
          .option("dbtable", s"$fullPath")
        try {
          jdbcWriter
            .save()
        } catch {
          case e: Exception =>
            println(e.getLocalizedMessage)
            println("Trying to create the database")
            JDBCTools.createJDBCDatabase(
              target.getConnection,
              fullPath.split("\\.")(0)
            )
            jdbcWriter
              .save()
        }
      } else {
        writerWithPartitioning
          .save(fullPath)
      }
    })
  }
}
