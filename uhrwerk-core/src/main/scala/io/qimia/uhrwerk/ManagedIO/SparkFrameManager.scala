package io.qimia.uhrwerk.ManagedIO

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.ManagedIO.SparkFrameManager.concatenatePaths
import io.qimia.uhrwerk.backend.service.dependency.BulkDependencyResult
import io.qimia.uhrwerk.config.{ConnectionType, PartitionUnit}
import io.qimia.uhrwerk.config.model._
import io.qimia.uhrwerk.utils.{JDBCTools, TimeTools}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}

object SparkFrameManager {
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
      loadDataFrameFromFileSystem(startTS, source.getPartitionSize, source.getConnection.getJdbcUrl,
        source.getPath, source.getFormat, dataFrameReaderOptions)
    }
  }

  /**
   * Returns a full location based on the parameters.
   *
   * @param startTS       Batch timestamp.
   * @param partitionSize Partition size, e.g 15m.
   * @param connectionUrl Connection Url, i.e. the full path prefix.
   * @param path          Path to the dataframe.
   * @return Full location.
   */
  def getFullLocation(startTS: Option[LocalDateTime],
                      partitionSize: Int,
                      partitionUnit: PartitionUnit,
                      connectionUrl: String,
                      path: String): String = {
    if (startTS.isDefined) {
      val duration = TimeTools.convertDurationStrToObj(partitionUnit, partitionSize)
      val date = TimeTools.dateTimeToDate(startTS.get)
      val batch = TimeTools.dateTimeToPostFix(startTS.get, duration)
      SparkFrameManager.concatenatePaths(connectionUrl, path, s"date=$date", s"batch=$batch")
    } else {
      SparkFrameManager.concatenatePaths(connectionUrl, path)
    }
  }

  /**
   * Loads a dataframe from a file system.
   *
   * @param startTS                Batch timestamp.
   * @param partitionSize          Partition size, e.g 15m.
   * @param connectionUrl          Connection Url, i.e. the full path prefix.
   * @param path                   Path to the dataframe.
   * @param format                 DataFrame format, specified according to spark docs, i.e. json, parquet, orc, libsvm, csv, text.
   * @param dataFrameWriterOptions Optional Spark reading options.
   * @return Loaded DataFrame.
   */
  private def loadDataFrameFromFileSystem(startTS: Option[LocalDateTime],
                                          partitionSize: Int,
                                          connectionUrl: String,
                                          path: String,
                                          format: String,
                                          dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    val fullLocation = getFullLocation(startTS, partitionSize, connectionUrl, path)

    val reader = if (dataFrameWriterOptions.isDefined) {
      sparkSession
        .read
        .options(dataFrameWriterOptions.get)
    } else {
      sparkSession
        .read
    }

    reader.format(format).load(fullLocation)
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
    assert(dependencyResult.getSucceeded.nonEmpty)

    val filter: Column = dependencyResult
      .getSucceeded
      .map(p => p match {
        case p.getMinute != null => col("minute") === p.getMinute
        case p.getHour != null => col("hour") === p.getHour
        case p.getDay != null => col("day") === p.getDay
        case p.getMonth != null => col("month") === p.getMonth
        case p.getYear != null => col("year") === p.getYear
        case _ => throw new Exception("SparkFrameManager received an empty partition")
      })
      .reduce((a, b) => a || b)

    // todo speedup when full year/month/day..

    val dfReader = sparkSession
      .read
      .format(dependencyResult.getDependency.getFormat)

    val dfReaderWithUserOptions = if (dataFrameReaderOptions.isDefined) {
      dfReader
        .options(dataFrameReaderOptions.get)
    } else {
      dfReader
    }

    val df = if (dependencyResult.getConnection.getType.equals(ConnectionType.JDBC)) {
      dfReaderWithUserOptions
        .option("url", dependencyResult.getConnection.getJdbcUrl)
        .option("driver", dependencyResult.getConnection.getJdbcDriver)
        .option("user", dependencyResult.getConnection.getJdbcUser)
        .option("password", dependencyResult.getConnection.getJdbcPass)
        .option("dbtable", dependencyResult.getDependency.getPath(false))
        .load()
    } else {
      dfReaderWithUserOptions
        .load(
          concatenatePaths(dependencyResult.getConnection.getPath,
            dependencyResult.getDependency.getPath(true)))
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

  /**
   * Loads multiple batches from datalake.
   *
   * @param conn          Connection
   * @param dependency    Location Info
   * @param startTS       Start Timestamp
   * @param endTSExcl     End Timestamp exclusive
   * @param batchDuration Batch Duration
   * @return DataFrame
   */
  def loadMoreBatches(conn: Connection,
                      dependency: Dependency,
                      startTS: LocalDateTime,
                      endTSExcl: LocalDateTime,
                      batchDuration: Duration): DataFrame = {
    import sparkSession.implicits._
    val loc = SparkFrameManager.concatenatePaths(conn.getJdbcUrl, dependency.getPath(true))
    val df = sparkSession.read.parquet(loc)
    val startRange = TimeTools.dateTimeToPostFix(startTS, batchDuration)
    val endRange = TimeTools.dateTimeToPostFix(endTSExcl, batchDuration)
    val startDate = TimeTools.dateTimeToDate(startTS)
    val endDate = TimeTools.dateTimeToDate(endTSExcl)

    val filtered = df
      .where(($"date" >= startDate) and ($"date" <= endDate))

    if (TimeTools.isDurationSizeDays(batchDuration)) {
      filtered
    } else {
      filtered
        .where(($"batch" >= startRange) and ($"batch" < endRange))
    }
  }

  /**
   * Save dataframe to datalake
   *
   * @param frame                  DataFrame to save
   * @param conn                   Connection
   * @param locationTargetInfo     Location Info
   * @param startTS                Batch Timestamp, optional
   * @param dataFrameWriterOptions Optional Spark writing options.
   */
  override def writeDataFrame(frame: DataFrame,
                              conn: Connection,
                              locationTargetInfo: Target,
                              locationTableInfo: Table,
                              startTS: Option[LocalDateTime] = Option.empty,
                              dataFrameWriterOptions: Option[Map[String, String]] = Option.empty): Unit = {
    assert(conn.getName == locationTargetInfo.getConnection.getName)

    if (conn.getType.equals(ConnectionType.JDBC)) {
      writeDataFrameToJDBC(frame, conn, locationTargetInfo, locationTableInfo, startTS, dataFrameWriterOptions)
    } else {
      val fullLocation = getFullLocation(startTS,
        locationTableInfo.getPartitionSize,
        conn.getJdbcUrl,
        locationTableInfo.getPath)

      val writer = frame.write.mode(SaveMode.Append).format(locationTargetInfo.getFormat)
      val writerWithOptions = if (dataFrameWriterOptions.isDefined) {
        writer
          .options(dataFrameWriterOptions.get)
      } else {
        writer
      }

      writerWithOptions.save(fullLocation)
    }
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
    assert(conn.getName == locationTargetInfo.getConnectionName)

    val (date: Option[String], batch: Option[String]) = if (startTS.isDefined) {
      val duration = TimeTools.convertDurationStrToObj(locationTableInfo.getPartitionSize)
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
      .option("user", conn.getUser)
      .option("password", conn.getPass)
      .option("dbtable", locationTableInfo.getPath)

    try {
      dfWriter
        .save()
    } catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println("Trying to create the database")
        JDBCTools.createJDBCDatabase(conn, locationTableInfo.getPath.split("\\.")(0))
        dfWriter
          .save()
    }
  }
}
