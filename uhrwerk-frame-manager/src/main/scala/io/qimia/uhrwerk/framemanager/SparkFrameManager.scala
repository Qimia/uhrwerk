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
  val timeColumns = List("year", "month", "day", "hour", "minute")
  val timeColumnsFormats = List("yyyy", "yyyy-MM", "yyyy-MM-dd", "yyyy-MM-dd-HH", "yyyy-MM-dd-HH-mm")

  /**
   * Concatenates paths into a single string. Handles properly all trailing slashes
   *
   * @param first First path
   * @param more  One or more paths
   * @return Concatenated path
   */
  def concatenatePaths(first: String, more: String*): String = {
    Paths.get(first, more: _*).toString
  }

  /**
   * Returns a full location based on the parameters.
   *
   * @param connectionPath Connection path.
   * @param tablePath      Table path.
   * @return Full location.
   */
  def getFullLocation(connectionPath: String, tablePath: String): String = {
    concatenatePaths(connectionPath, tablePath)
  }

  def concatenateDateParts(first: String, second: String): String = {
    first + "-" + second
  }

  def getTimeValues(startTS: LocalDateTime): (String, String, String, String, String) = {
    val year = startTS.getYear.toString
    val month = concatenateDateParts(year, TimeTools.leftPad(startTS.getMonthValue.toString))
    val day = concatenateDateParts(month, TimeTools.leftPad(startTS.getDayOfMonth.toString))
    val hour = concatenateDateParts(day, TimeTools.leftPad(startTS.getHour.toString))
    val minute = concatenateDateParts(hour, TimeTools.leftPad(startTS.getMinute.toString))

    (year, month, day, hour, minute)
  }

  def createDatePath(startTS: LocalDateTime): String = {
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
  def getTablePath(table: Table, fileSystem: Boolean, format: String): String = {
    if (fileSystem) {
      Paths.get(s"area=${table.getArea}",
        s"vertical=${table.getVertical}",
        s"table=${table.getName}",
        s"version=${table.getVersion}",
        s"format=$format")
        .toString
    }
    else { // jdbc
      table.getArea + "-" + table.getVertical + "." + table.getName + "-" + table.getVersion
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
  def getDependencyPath(dependency: Dependency, fileSystem: Boolean): String = {
    if (fileSystem) {
      Paths.get(s"area=${dependency.getArea}",
        s"vertical=${dependency.getVertical}",
        s"table=${dependency.getTableName}",
        s"version=${dependency.getVersion}",
        s"format=${dependency.getFormat}")
        .toString
    }
    else { // jdbc
      dependency.getArea + "-" + dependency.getVertical + "." + dependency.getTableName + "-" + dependency.getVersion
    }
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
   */
  override def loadSourceDataFrame(source: Source,
                                   startTS: Option[LocalDateTime] = Option.empty,
                                   endTSExcl: Option[LocalDateTime] = Option.empty,
                                   dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
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

    val df = reader.format(source.getFormat).load(fullLocation) // todo change to getFormat

    if (startTS.isDefined && endTSExcl.isDefined) {
      df
        .filter(col(source.getSelectColumn) >= startTS.get)
        .filter(col(source.getSelectColumn) < endTSExcl.get)
    } else if (startTS.isDefined) {
      df
        .filter(col(source.getSelectColumn) >= startTS.get)
    } else if (endTSExcl.isDefined) {
      df
        .filter(col(source.getSelectColumn) < endTSExcl.get)
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
      .map(p => p match {
        case p if p.getMinute != null => col("minute") === s"${p.getYear}-${TimeTools.leftPad(p.getMonth)}-${TimeTools.leftPad(p.getDay)}-${TimeTools.leftPad(p.getHour)}-${TimeTools.leftPad(p.getMinute)}"
        case p if p.getHour != null => col("hour") === s"${p.getYear}-${TimeTools.leftPad(p.getMonth)}-${TimeTools.leftPad(p.getDay)}-${TimeTools.leftPad(p.getHour)}"
        case p if p.getDay != null => col("day") === s"${p.getYear}-${TimeTools.leftPad(p.getMonth)}-${TimeTools.leftPad(p.getDay)}"
        case p if p.getMonth != null => col("month") === s"${p.getYear}-${TimeTools.leftPad(p.getMonth)}"
        case p if p.getYear != null => col("year") === s"${p.getYear}"
        case _ => throw new Exception("SparkFrameManager received an empty partition")
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
        .option("dbtable", getFullLocation(dependencyResult.connection.getPath, getDependencyPath(dependencyResult.dependency, false)))
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
        JDBCTools.queryTable(source.getSelectQuery, startTS, endTSExcl)

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
      } else if (source.getSelectColumn.nonEmpty && startTS.isDefined && endTSExcl.isDefined) {
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

  def containsTimeColumns(df: DataFrame): Boolean = {
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
   * @param dataFrameWriterOptions Optional Spark writing options.
   */
  override def writeDataFrame(frame: DataFrame,
                              locationTableInfo: Table,
                              startTS: Option[LocalDateTime] = Option.empty,
                              dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): Unit = {
    locationTableInfo.getTargets.foreach(target => {
      val isJDBC = target.getConnection.getType.equals(ConnectionType.JDBC)
      val path = getFullLocation(target.getConnection.getPath, getTablePath(locationTableInfo, !isJDBC, target.getFormat))

      val (fullPath, df) = if (startTS.isDefined) {
        val datePath = createDatePath(startTS.get)

        (concatenatePaths(path, datePath), frame.drop(timeColumns: _*))
      } else {
        (path, frame)
      }

      if (isJDBC) {
        writeDataFrameToJDBC(df, target.getConnection, target, locationTableInfo, startTS, dataFrameWriterOptions) // todo implement
      } else {
        val writer = df.write.mode(SaveMode.Append).format(target.getFormat)
        val writerWithOptions = if (dataFrameWriterOptions.isDefined) {
          writer
            .options(dataFrameWriterOptions.get)
        } else {
          writer
        }

        val writerWithPartitioning = if (startTS.isEmpty && containsTimeColumns(df)) {
          writerWithOptions
            .partitionBy(timeColumns: _*)
        } else {
          writerWithOptions
        }

        println(s"Saving DF to $fullPath")
        writerWithPartitioning.save(fullPath)
      }
    })
  }

  /**
   * Write dataframe to JDBC. Either one batch or the full path.
   *
   * @param frame                  DataFrame to save
   * @param conn                   Connection
   * @param locationTargetInfo     Location Info
   * @param startTS                Batch Timestamp, optional
   * @param dataFrameWriterOptions Optional Spark writing options.
   */
  private def writeDataFrameToJDBC(frame: DataFrame,
                                   conn: Connection,
                                   locationTargetInfo: Target,
                                   locationTableInfo: Table,
                                   startTS: Option[LocalDateTime],
                                   dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): Unit = {
    val (date: Option[String], batch: Option[String]) = if (startTS.isDefined) {
      val duration = TimeTools.convertDurationStrToObj(locationTableInfo.getPartitionUnit, locationTableInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(startTS.get)
      val batch = TimeTools.dateTimeToPostFix(startTS.get, duration)
      (Option(date), Option(batch))
    } else {
      (Option.empty, Option.empty)
    }

    val df = if (date.isDefined && batch.isDefined) {
      frame
        .withColumn("date", lit(date.get))
        .withColumn("batch", lit(batch.get))
    } else {
      frame
    }

    val dfWriterWithUserOptions = if (dataFrameWriterOptions.isDefined) {
      df
        .write
        .options(dataFrameWriterOptions.get)
    } else {
      df
        .write
    }

    val dfWriter: DataFrameWriter[Row] = dfWriterWithUserOptions
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", conn.getJdbcUrl)
      .option("driver", conn.getJdbcDriver)
    //      .option("user", conn.getUser)
    //      .option("password", conn.getPass)
    //      .option("dbtable", locationTableInfo.getPath)

    try {
      dfWriter
        .save()
    } catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println("Trying to create the database")
        //        JDBCTools.createJDBCDatabase(conn, locationTableInfo.getPath.split("\\.")(0))
        dfWriter
          .save()
    }
  }
}
