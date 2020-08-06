package io.qimia.uhrwerk.ManagedIO

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.ConnectionType
import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.{JDBCTools, TimeTools}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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
  override def loadDataFrame(conn: Connection,
                             locationInfo: Dependency,
                             batchTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
      loadDFFromJDBC(conn, locationInfo, batchTS)
    } else {
      // aggregates
      if (batchTS.isDefined && locationInfo.getPartitionCount > 1) {
        val (startTS, endTSExcl) = TimeTools.getRangeFromAggregate(batchTS.get, locationInfo.getPartitionSize, locationInfo.getPartitionCount)
        loadDataFrames(conn, locationInfo, startTS, endTSExcl, locationInfo.getPartitionSizeDuration)
      } else {
        val fullLocation = if (batchTS.isDefined) {
          val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
          val date = TimeTools.dateTimeToDate(batchTS.get)
          val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
          SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath, s"date=$date", s"batch=$batch")

        } else {
          SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath)
        }
        sparkSession.read.parquet(fullLocation)
      }
    }
  }

  /**
   * Load dataframe from JDBC. Either one batch or the full path.
   *
   * @param conn         Connection
   * @param locationInfo Location Info
   * @param batchTS      Batch Timestamp. If not defined, load the full path.
   * @return DataFrame
   */
  private def loadDFFromJDBC(conn: Connection,
                             locationInfo: Dependency,
                             batchTS: Option[LocalDateTime]): DataFrame = {
    import sparkSession.implicits._
    assert(conn.getName == locationInfo.getConnectionName)

    val (date: Option[String], batch: Option[String]) = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(batchTS.get)
      val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
      (Option(date), Option(batch))
    } else {
      (Option.empty, Option.empty)
    }
    val df = sparkSession
      .read
      .format("jdbc")
      .option("url", conn.getJdbcUrl)
      .option("driver", conn.getJdbcDriver)
      .option("user", conn.getUser)
      .option("password", conn.getPass)
      .option("dbtable", s"${locationInfo.getArea}.${locationInfo.getPath}") // schema.tableName ?
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
  def loadDataFrames(conn: Connection,
                     locationInfo: Dependency,
                     startTS: LocalDateTime,
                     endTSExcl: LocalDateTime,
                     batchDuration: Duration): DataFrame = {
    import sparkSession.implicits._
    val loc = SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath)
    val df = sparkSession.read.parquet(loc)
    val startRange = TimeTools.dateTimeToPostFix(startTS, batchDuration)
    val endRange = TimeTools.dateTimeToPostFix(endTSExcl, batchDuration)
    val startDate = TimeTools.dateTimeToDate(startTS)
    val endDate = TimeTools.dateTimeToDate(endTSExcl)
    df
      .where(($"date" >= startDate) and ($"date" <= endDate))
      .where(($"batch" >= startRange) and ($"batch" < endRange))
  }

  /**
   * Save dataframe to datalake
   *
   * @param frame        DataFrame to save
   * @param conn         Connection
   * @param locationInfo Location Info
   * @param batchTS      Batch Timestamp, optional.
   */
  override def writeDataFrame(frame: DataFrame,
                              conn: Connection,
                              locationInfo: Target,
                              batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)

    if (conn.getTypeEnum.equals(ConnectionType.JDBC)) {
      writeDFToJDBC(frame, conn, locationInfo, batchTS)
    } else {
      val fullLocation = if (batchTS.isDefined) {
        val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
        val date = TimeTools.dateTimeToDate(batchTS.get)
        val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
        SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath, s"date=$date", s"batch=$batch")
      } else {
        SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath)
      }
      frame.write.mode(SaveMode.Append).parquet(fullLocation)
    }
  }

  /**
   * Write dataframe to JDBC. Either one batch or the full path.
   *
   * @param frame        DataFrame to save
   * @param conn         Connection
   * @param locationInfo Location Info
   * @param batchTS      Batch Timestamp, optional.
   */
  private def writeDFToJDBC(frame: DataFrame,
                            conn: Connection,
                            locationInfo: Target,
                            batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)

    val (date: Option[String], batch: Option[String]) = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
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

    val dfWriter = df
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", conn.getJdbcUrl)
      .option("driver", conn.getJdbcDriver)
      .option("user", conn.getUser)
      .option("password", conn.getPass)
      .option("dbtable", s"${locationInfo.getArea}.${locationInfo.getPath}") // schema.tableName ?

    try {
      dfWriter
        .save()
    } catch {
      case e: Exception =>
        println(e.getLocalizedMessage)
        println("Trying to create the database")
        JDBCTools.createJDBCDatabase(conn, locationInfo)
        dfWriter
          .save()
    }
  }
}
