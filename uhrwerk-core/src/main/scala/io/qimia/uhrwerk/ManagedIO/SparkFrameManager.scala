package io.qimia.uhrwerk.ManagedIO

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.config.model._
import io.qimia.uhrwerk.config.{ConnectionType, PartitionTransformType}
import io.qimia.uhrwerk.utils.{JDBCTools, TimeTools}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

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

  // TODO: This more of a rough sketch than the actual full FrameManager. We will need to handle different batch sizes

  /**
   * Loads a source dataframe. Either one batch or the full path.
   *
   * @param conn                   Connection
   * @param source                 Source
   * @param startTS                Batch Timestamp. If not defined, load the full path.
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return DataFrame
   */
  override def loadSourceDataFrame(conn: Connection,
                                   source: Source,
                                   startTS: Option[LocalDateTime] = Option.empty,
                                   dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    assert(conn.getName == source.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
      loadDFFromJDBC(conn,
        source.getPartitionSize,
        source.getSelectQuery,
        source.getPartitionQuery,
        source.getPartitionColumn,
        source.getPath,
        startTS)
    } else {
      loadDataFrameFromFileSystem(startTS, source.getPartitionSize, conn.getJdbcUrl,
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
                      partitionSize: String,
                      connectionUrl: String,
                      path: String): String = {
    if (startTS.isDefined) {
      val duration = TimeTools.convertDurationStrToObj(partitionSize)
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
                                          partitionSize: String,
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
   * Loads a dependency dataframe. Either one batch or the full path.
   *
   * @param conn                   Connection
   * @param dependency             Dependency
   * @param startTS                Batch Timestamp. If not defined, load the full path.
   * @param dataFrameReaderOptions Optional Spark reading options.
   * @return DataFrame
   */
  override def loadDependencyDataFrame(conn: Connection,
                                       dependency: Dependency,
                                       startTS: Option[LocalDateTime],
                                       dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    assert(conn.getName == dependency.getConnectionName)

    // aggregates
    if (startTS.isDefined && dependency.getTypeEnum.equals(PartitionTransformType.AGGREGATE)) {
      val (startTSAgg, endTSExcl) = TimeTools.getRangeFromAggregate(startTS.get, dependency.getPartitionSize, dependency.getPartitionCount)
      loadMoreBatches(conn, dependency, startTSAgg, endTSExcl, dependency.getPartitionSizeDuration)
      // windows
    } else if (startTS.isDefined && dependency.getTypeEnum.equals(PartitionTransformType.WINDOW)) {
      val (startTSWindow, endTSExcl) = TimeTools.getRangeFromWindow(startTS.get, dependency.getPartitionSize, dependency.getPartitionCount)
      loadMoreBatches(conn, dependency, startTSWindow, endTSExcl, dependency.getPartitionSizeDuration)
    } else {
      loadDataFrameFromFileSystem(startTS, dependency.getPartitionSize, conn.getJdbcUrl,
        dependency.getPath(true), dependency.getFormat, dataFrameReaderOptions)
    }
  }

  /**
   * Loads a dataframe from JDBC. Either one batch or the full path.
   * If the selectQuery is set, it uses that instead of the path.
   * If the partitionQuery is set, it first runs this to get the partition boundaries.
   *
   * @param connection      Connection
   * @param partitionSize   Partition size, e.g. 30m.
   * @param selectQuery     Select query.
   * @param partitionQuery  Partition query.
   * @param partitionColumn Partition column.
   * @param path            Path (schema + table name)
   * @param startTS         Batch Timestamp. If not defined, load the full path.
   * @return DataFrame
   */
  private def loadDFFromJDBC(connection: Connection,
                             partitionSize: String,
                             selectQuery: String,
                             partitionQuery: String,
                             partitionColumn: String,
                             path: String,
                             startTS: Option[LocalDateTime]): DataFrame = {
    import sparkSession.implicits._
    assert(connection.getName == connection.getName)

    val (date: Option[String], batch: Option[String]) = if (startTS.isDefined) {
      val duration = TimeTools.convertDurationStrToObj(partitionSize)
      val date = TimeTools.dateTimeToDate(startTS.get)
      val batch = TimeTools.dateTimeToPostFix(startTS.get, duration)
      (Option(date), Option(batch))
    } else {
      (Option.empty, Option.empty)
    }

    val dfReader: DataFrameReader = JDBCTools.getDbConfig(sparkSession, connection)

    val dfReaderWithQuery: DataFrameReader = if (selectQuery.nonEmpty) {
      val query: String =
        JDBCTools.queryTable(selectQuery, startTS)

      val dfReaderWithQuery = dfReader
        .option("dbtable", query)
      //        .option("numPartitions", locationInfo.getNumPartitions) todo add

      if (partitionQuery.nonEmpty) {
        val (minId, maxId) = JDBCTools.minMaxQueryIds(sparkSession,
          connection,
          partitionColumn,
          partitionQuery,
          startTS)

        dfReaderWithQuery
          .option("partitionColumn", partitionColumn)
          .option("lowerBound", minId)
          .option("upperBound", maxId)
      } else {
        dfReaderWithQuery
      }
    } else {
      dfReader
        .option("dbtable", path) // area-vertical.tableName-version
    }

    val df = dfReaderWithQuery
      .load()

    if (date.isDefined && batch.isDefined) {
      df
        .where($"date" === date.get)
        .where($"batch" === batch.get)
        .drop("date", "batch")
    } else {
      df
    }
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
    assert(conn.getName == locationTargetInfo.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
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
