package io.qimia.uhrwerk.ManagedIO

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.config.model.{Connection, Dependency, Table, TableInput, Target}
import io.qimia.uhrwerk.config.{ConnectionType, DependencyType}
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
   * Load dataframe from datalake. Either one batch or the full path.
   *
   * @param conn         Connection
   * @param locationInfo Location Info
   * @param batchTS      Batch Timestamp. If not defined, load the full path.
   * @return DataFrame
   */
  override def loadDataFrame[T <: TableInput](conn: Connection,
                                              locationInfo: T,
                                              batchTS: Option[LocalDateTime]): DataFrame = {
    val dependency: Option[Dependency] = locationInfo match {
      case _: Dependency =>
        Some(locationInfo.asInstanceOf[Dependency])
      case _: Any =>
        Option.empty
    }
    assert(conn.getName == locationInfo.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
      loadDFFromJDBC(conn, locationInfo, batchTS)
    } else {
      // aggregates
      if (dependency.isDefined && batchTS.isDefined && dependency.get.getTypeEnum.equals(DependencyType.AGGREGATE)) {
        val (startTS, endTSExcl) = TimeTools.getRangeFromAggregate(batchTS.get, dependency.get.getPartitionSize, dependency.get.getPartitionCount)
        loadMoreBatches(conn, dependency.get, startTS, endTSExcl, dependency.get.getPartitionSizeDuration)
        // windows
      } else if (dependency.isDefined && batchTS.isDefined && dependency.get.getTypeEnum.equals(DependencyType.WINDOW)) {
        val (startTS, endTSExcl) = TimeTools.getRangeFromWindow(batchTS.get, dependency.get.getPartitionSize, dependency.get.getPartitionCount)
        loadMoreBatches(conn, dependency.get, startTS, endTSExcl, dependency.get.getPartitionSizeDuration)
      } else {
        val fullLocation = if (batchTS.isDefined) {
          val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
          val date = TimeTools.dateTimeToDate(batchTS.get)
          val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
          SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getFormat, s"date=$date", s"batch=$batch")

        } else {
          SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getFormat)
        }
        sparkSession.read.parquet(fullLocation)
      }
    }
  }

  /**
   * Loads a dataframe from JDBC. Either one batch or the full path.
   * If the selectQuery is set, it uses that instead of the path.
   * If the partitionQuery is set, it first runs this to get the partition boundaries.
   *
   * @param connection   Connection
   * @param locationInfo Location Info
   * @param batchTS      Batch Timestamp. If not defined, load the full path.
   * @return DataFrame
   */
  private def loadDFFromJDBC(connection: Connection,
                             locationInfo: TableInput,
                             batchTS: Option[LocalDateTime]): DataFrame = {
    import sparkSession.implicits._
    assert(connection.getName == locationInfo.getConnectionName)

    val (date: Option[String], batch: Option[String]) = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(batchTS.get)
      val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
      (Option(date), Option(batch))
    } else {
      (Option.empty, Option.empty)
    }

    val dfReader: DataFrameReader = JDBCTools.getDbConfig(sparkSession, connection)

    val dfReaderWithQuery: DataFrameReader = if (locationInfo.getSelectQuery.nonEmpty) {
      val query: String =
        JDBCTools.queryTable(locationInfo.getSelectQuery, batchTS)

      val dfReaderWithQuery = dfReader
        .option("dbtable", query)
      //        .option("numPartitions", locationInfo.getNumPartitions) todo add

      if (locationInfo.getPartitionQuery.nonEmpty) {
        val (minId, maxId) = JDBCTools.minMaxQueryIds(sparkSession,
          connection,
          locationInfo.getPartitionColumn,
          locationInfo.getPartitionQuery,
          batchTS)

        dfReaderWithQuery
          .option("partitionColumn", locationInfo.getPartitionColumn)
          .option("lowerBound", minId)
          .option("upperBound", maxId)
      } else {
        dfReaderWithQuery
      }
    } else {
      dfReader
        .option("dbtable", s"${locationInfo.getFormat}") // schema.tableName
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
   * @param locationInfo  Location Info
   * @param startTS       Start Timestamp
   * @param endTSExcl     End Timestamp exclusive
   * @param batchDuration Batch Duration
   * @return DataFrame
   */
  def loadMoreBatches(conn: Connection,
                      locationInfo: Dependency,
                      startTS: LocalDateTime,
                      endTSExcl: LocalDateTime,
                      batchDuration: Duration): DataFrame = {
    import sparkSession.implicits._
    val loc = SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getFormat)
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
   * @param frame        DataFrame to save
   * @param conn         Connection
   * @param locationTargetInfo Location Info
   * @param batchTS      Batch Timestamp, optional
   */
  override def writeDataFrame(frame: DataFrame,
                              conn: Connection,
                              locationTargetInfo: Target,
                              locationTableInfo: Table,
                              batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationTargetInfo.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
      writeDFToJDBC(frame, conn, locationTargetInfo, locationTableInfo, batchTS)
    } else {
      val fullLocation = if (batchTS.isDefined) {
        val duration = TimeTools.convertDurationToObj(locationTableInfo.getPartitionSize)
        val date = TimeTools.dateTimeToDate(batchTS.get)
        val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
        SparkFrameManager.concatenatePaths(conn.getStartPath, locationTargetInfo.getFormat, s"date=$date", s"batch=$batch")
      } else {
        SparkFrameManager.concatenatePaths(conn.getStartPath, locationTargetInfo.getFormat)
      }
      frame.write.mode(SaveMode.Append).parquet(fullLocation)
    }
  }

  /**
   * Write dataframe to JDBC. Either one batch or the full path.
   *
   * @param frame        DataFrame to save
   * @param conn         Connection
   * @param locationTargetInfo Location Info
   * @param batchTS      Batch Timestamp, optional
   */
  private def writeDFToJDBC(frame: DataFrame,
                            conn: Connection,
                            locationTargetInfo: Target,
                            locationTableInfo: Table,
                            batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationTargetInfo.getConnectionName)

    val (date: Option[String], batch: Option[String]) = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationTableInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(batchTS.get)
      val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
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

    val dfWriter: DataFrameWriter[Row] = df
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", conn.getConnectionUrl)
      .option("driver", conn.getJdbcDriver)
      .option("user", conn.getUser)
      .option("password", conn.getPass)
      .option("dbtable", s"${locationTargetInfo.getFormat}") // schema.tableName

    try {
      dfWriter
        .save()
    } catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println("Trying to create the database")
        JDBCTools.createJDBCDatabase(conn, locationTargetInfo.getFormat.split("\\.")(0))
        dfWriter
          .save()
    }
  }
}
