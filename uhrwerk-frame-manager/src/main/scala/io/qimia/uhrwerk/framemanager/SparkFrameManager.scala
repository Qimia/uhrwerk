package io.qimia.uhrwerk.framemanager

import java.nio.file.Paths
import java.time.LocalDateTime

import io.qimia.uhrwerk.common.framemanager.{BulkDependencyResult, FrameManager}
import io.qimia.uhrwerk.common.model._
import io.qimia.uhrwerk.framemanager.SparkFrameManager._
import io.qimia.uhrwerk.utils.{JDBCTools, TimeTools}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, to_date}

object SparkFrameManager {
  private[framemanager] val timeColumns = List("year", "month", "day", "hour", "minute")
  private[framemanager] val timeColumnsFormats = List("yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd-HH", "yyyy-MM-dd-HH-mm")

  /**
   * Concatenates paths into a single string. Handles properly all trailing slashes
   *
   * @param first First path
   * @param more  One or more paths
   * @return Concatenated path
   */
  private[framemanager] def concatenatePaths(first: String, more: String*): String = {
    Paths.get(first, more: _*).toString
  }

  /**
   * Returns a full location based on the parameters.
   *
   * @param connectionPath Connection path.
   * @param tablePath      Table path.
   * @return Full location.
   */
  private[framemanager] def getFullLocation(connectionPath: String, tablePath: String): String = {
    concatenatePaths(connectionPath, tablePath)
  }

  private[framemanager] def concatenateDateParts(first: String, second: String): String = {
    first + "-" + second
  }

  private[framemanager] def getTimeValues(startTS: LocalDateTime): (String, String, String, String, String) = {
    val year = startTS.getYear.toString
    val month = concatenateDateParts(year, TimeTools.leftPad(startTS.getMonthValue.toString))
    val day = concatenateDateParts(month, TimeTools.leftPad(startTS.getDayOfMonth.toString))
    val hour = concatenateDateParts(day, TimeTools.leftPad(startTS.getHour.toString))
    val minute = concatenateDateParts(hour, TimeTools.leftPad(startTS.getMinute.toString))

    (year, month, day, hour, minute)
  }

  private[framemanager] def createDatePath(startTS: LocalDateTime): String = {
    val (year, month, day, hour, minute) = getTimeValues(startTS)

    concatenatePaths(s"year=$year",
      s"month=$month",
      s"day=$day",
      s"hour=$hour",
      s"minute=$minute")
  }

  /**
   * Concatenates area, vertical, table, version, and format into a path.
   * Either with slashes for a file system or with dashes and a dot for jdbc.
   *
   * @param table      Table.
   * @param fileSystem Whether the path is for a file system or for jdbc.
   * @param format     Target's format.
   * @return The concatenated path.
   */
  private[framemanager] def getTablePath(table: Table, fileSystem: Boolean, format: String): String = {
    if (fileSystem) {
      Paths.get(s"area=${table.getArea}",
        s"vertical=${table.getVertical}",
        s"table=${table.getName}",
        s"version=${table.getVersion}",
        s"format=$format")
        .toString
    }
    else { // jdbc
      "`" + table.getArea + "-" + table.getVertical + "`.`" + table.getName + "-" + table.getVersion.replace(".", "_") + "`"
    }
  }

  /**
   * Concatenates area, vertical, table, version, and format into a path.
   * Either with slashes for a file system or with dashes and a dot for jdbc.
   *
   * @param dependency Dependency.
   * @param fileSystem Whether the path is for a file system or for jdbc.
   * @return The concatenated path.
   */
  private[framemanager] def getDependencyPath(dependency: Dependency, fileSystem: Boolean): String = {
    if (fileSystem) {
      Paths.get(s"area=${dependency.getArea}",
        s"vertical=${dependency.getVertical}",
        s"table=${dependency.getTableName}",
        s"version=${dependency.getVersion}",
        s"format=${dependency.getFormat}")
        .toString
    }
    else { // jdbc
      "`" + dependency.getArea + "-" + dependency.getVertical + "`.`" + dependency.getTableName + "-" + dependency.getVersion.replace(".", "_") + "`"
    }
  }

  /**
   * Checks whether a string is empty (or null because of Java classes).
   *
   * @param s String to check.
   * @return True if null or empty.
   */
  private[framemanager] def isStringEmpty(s: String): Boolean = {
    s == null || s.isEmpty
  }

  /**
   * Checks whether a DataFrame contains all time columns.
   *
   * @param df DataFrame.
   * @return True if the df contains all time columns with proper formatting.
   */
  private[framemanager] def containsTimeColumns(df: DataFrame): Boolean = {
    if (!timeColumns.forall(df.columns.contains(_))) {
      return false
    }
    df.cache()

    if (timeColumns.zip(timeColumnsFormats).forall(p => {
      df
        .withColumn(p._1 + "_transformed", to_date(col(p._1), p._2))
        .filter(col(p._1 + "_transformed").isNull)
        .count == 0
    })) {
      return true
    }
    false
  }

  /**
   * Adds time columns to a DataFrame from a specified timestamp.
   *
   * @param frame DataFrame to add columns to.
   * @param ts    TimeStamp.
   * @return DataFrame with the time columns.
   */
  private[framemanager] def addTimeColumns(frame: DataFrame, ts: LocalDateTime): DataFrame = {
    val (year, month, day, hour, minute) = getTimeValues(ts)

    frame
      .withColumn("year", lit(year))
      .withColumn("month", lit(month))
      .withColumn("day", lit(day))
      .withColumn("hour", lit(hour))
      .withColumn("minute", lit(minute))
  }
}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {

  /**
   * Loads a source dataframe. Either one batch or the full path.
   *
   * @param source                 Source
   * @param startTS                Batch Timestamp. If not defined, load the full path.
   * @param endTSExcl              End Timestamp exclusive
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return DataFrame
   * @throws IllegalArgumentException In case one or both timestamps are specified but the source select column is empty
   */
  override def loadSourceDataFrame(source: Source,
                                   startTS: Option[LocalDateTime] = Option.empty,
                                   endTSExcl: Option[LocalDateTime] = Option.empty,
                                   dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    if ((startTS.isDefined || endTSExcl.isDefined) && isStringEmpty(source.getSelectColumn)) {
      throw new IllegalArgumentException("When one or both of the timestamps are specified, " +
        "the source.selectColumn needs to be set as well.")
    }

    if (source.getConnection.getType.equals(ConnectionType.JDBC)) {
      loadSourceFromJDBC(source, startTS, endTSExcl, dataFrameReaderOptions)
    } else {
      loadDataFrameFromFileSystem(source, startTS, endTSExcl, dataFrameReaderOptions)
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
  private def loadDataFrameFromFileSystem(source: Source,
                                          startTS: Option[LocalDateTime] = Option.empty,
                                          endTSExcl: Option[LocalDateTime] = Option.empty,
                                          dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    val fullLocation = getFullLocation(source.getConnection.getPath, source.getPath)

    val reader = if (dataFrameReaderOptions.isDefined) {
      sparkSession
        .read
        .options(dataFrameReaderOptions.get)
    } else {
      sparkSession
        .read
    }

    val df = reader.format(source.getFormat).load(fullLocation)

    if (startTS.isDefined && endTSExcl.isDefined) {
      df
        .filter(col(source.getSelectColumn) >= TimeTools.convertTSToString(startTS.get))
        .filter(col(source.getSelectColumn) < TimeTools.convertTSToString(endTSExcl.get))
    } else if (startTS.isDefined) {
      df
        .filter(col(source.getSelectColumn) >= TimeTools.convertTSToString(startTS.get))
    } else if (endTSExcl.isDefined) {
      df
        .filter(col(source.getSelectColumn) < TimeTools.convertTSToString(endTSExcl.get))
    } else {
      df
    }
  }

  /**
   * Loads all partitions from the succeeded partitions of the DependencyResult
   *
   * @param dependencyResult       DependencyResult
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return DataFrame
   */
  override def loadDependencyDataFrame(dependencyResult: BulkDependencyResult,
                                       dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    assert(dependencyResult.succeeded.nonEmpty)

    val filter: Column = dependencyResult
      .succeeded
      .map(p => {
        val (year, month, day, hour, minute) = getTimeValues(p.getPartitionTs)
        p.getPartitionUnit match {
          case PartitionUnit.MINUTES => col("minute") === s"$minute"
          case PartitionUnit.HOURS => col("hour") === s"$hour"
          case _ => col("day") === s"$day"
        }
      })
      .reduce((a, b) => a || b)

    // todo speedup when full year/month/day..

    val dfReader = sparkSession
      .read
      .format(dependencyResult.dependency.getFormat)

    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val df = if (dependencyResult.connection.getType.equals(ConnectionType.JDBC)) {
      dfReaderWithUserOptions
        .option("url", dependencyResult.connection.getJdbcUrl)
        .option("driver", dependencyResult.connection.getJdbcDriver)
        .option("user", dependencyResult.connection.getJdbcUser)
        .option("password", dependencyResult.connection.getJdbcPass)
        .option("dbtable", getDependencyPath(dependencyResult.dependency, false))
        .load()
    } else {
      dfReaderWithUserOptions
        .load(
          getFullLocation(dependencyResult.connection.getPath, getDependencyPath(dependencyResult.dependency, true)))
    }

    df
      .filter(filter)
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
  private def loadSourceFromJDBC(source: Source,
                                 startTS: Option[LocalDateTime] = Option.empty,
                                 endTSExcl: Option[LocalDateTime] = Option.empty,
                                 dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {

    val dfReader: DataFrameReader = JDBCTools.getDbConfig(sparkSession, source.getConnection)
    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val dfReaderWithQuery: DataFrameReader = if (source.getSelectQuery.nonEmpty) {
      val query: String =
        JDBCTools.fillInQuery(source.getSelectQuery, startTS, endTSExcl)

      val dfReaderWithQuery = dfReaderWithUserOptions
        .option("dbtable", query)
        .option("numPartitions", source.getParallelLoadNum) // todo is this what I think it is?

      if (source.getParallelLoadQuery.nonEmpty) {
        val (minId, maxId) = JDBCTools.minMaxQueryIds(sparkSession,
          source.getConnection,
          source.getParallelLoadColumn,
          source.getParallelLoadQuery,
          startTS,
          endTSExcl)

        dfReaderWithQuery
          .option("partitionColumn", source.getParallelLoadColumn)
          .option("lowerBound", minId)
          .option("upperBound", maxId)
      } else if (!isStringEmpty(source.getSelectColumn) && startTS.isDefined && endTSExcl.isDefined) {
        dfReaderWithQuery
          .option("partitionColumn", source.getSelectColumn)
          .option("lowerBound", TimeTools.convertTSToString(startTS.get))
          .option("upperBound", TimeTools.convertTSToString(endTSExcl.get))
      } else {
        dfReaderWithQuery
      }
    } else {
      dfReader
        .option("dbtable", source.getPath) // area-vertical.tableName-version
    }

    val df: DataFrame = dfReaderWithQuery
      .load()

    df
  }

  /**
   * Saves a table to all its targets.
   * Four possible scenarios regarding the timestamp:
   * 1. startTS is provided and time columns (year, month, day, hour, minute) are in the DF => those columns are dropped
   * 2. startTS is provided and time columns are not in the DF => the DF is normally saved
   * 3. startTS is not provided and time columns with a proper format are in the DF => those columns are used as partitioning
   * 4. startTS is not provided and time columns are not in the DF => no partitioning is used
   *
   * @param frame                  DataFrame to save
   * @param locationTableInfo      Location Info
   * @param startTS                Start Timestamp, optional
   * @param dataFrameWriterOptions Optional array of Spark writing options.
   *                               If the array has only one item (one map), this one is used for all targets.
   *                               If the array has as many items as there are targets,
   *                               each target is saved with different options.
   */
  override def writeDataFrame(frame: DataFrame,
                              locationTableInfo: Table,
                              startTS: Option[LocalDateTime] = Option.empty,
                              dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty): Unit = {
    locationTableInfo.getTargets.zipWithIndex.foreach((item: (Target, Int)) => {
      val target: Target = item._1
      val index: Int = item._2
      if (target.getConnection == null) {
        throw new IllegalArgumentException("A connection is missing in the target.")
      }
      val isJDBC = target.getConnection.getType.equals(ConnectionType.JDBC)
      val tablePath = getTablePath(locationTableInfo, !isJDBC, target.getFormat)
      val path = if (isJDBC) {
        tablePath
      } else {
        getFullLocation(target.getConnection.getPath, tablePath)
      }

      val (fullPath, df) = if (startTS.isDefined) {
        val datePath = createDatePath(startTS.get)

        if (isJDBC) {
          val jdbcDF = addTimeColumns(frame, startTS.get)

          (path, jdbcDF)
        } else {
          (concatenatePaths(path, datePath), frame.drop(timeColumns: _*))
        }
      } else {
        (path, frame)
      }

      val writer = df.write.mode(SaveMode.Append).format(target.getFormat)
      val writerWithOptions = if (dataFrameWriterOptions.isDefined) {
        val options = if (dataFrameWriterOptions.get.length == locationTableInfo.getTargets.length) {
          dataFrameWriterOptions.get(index)
        } else {
          dataFrameWriterOptions.get.head
        }
        writer
          .options(options)
      } else {
        writer
      }

      val writerWithPartitioning = if ((startTS.isEmpty || isJDBC) && containsTimeColumns(df)) {
        writerWithOptions
          .partitionBy(timeColumns: _*)
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
            JDBCTools.createJDBCDatabase(target.getConnection, fullPath.split("\\.")(0))
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
