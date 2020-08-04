package io.qimia.uhrwerk.ManagedIO

import java.nio.file.Paths
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  override def loadDFFromLake(conn: Connection,
                              locationInfo: Dependency,
                              batchTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(batchTS.get)
      val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
      SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath, s"date=${date}", s"batch=${batch}")
    } else {
      SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath)
    }
    sparkSession.read.parquet(fullLocation)
  }

  // Testcode - How would loading multiple tables look like
  def loadDFsFromLake(conn: Connection,
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
  override def writeDFToLake(frame: DataFrame,
                             conn: Connection,
                             locationInfo: Target,
                             batchTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if (batchTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      val date = TimeTools.dateTimeToDate(batchTS.get)
      val batch = TimeTools.dateTimeToPostFix(batchTS.get, duration)
      SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath, s"date=${date}", s"batch=${batch}")
    } else {
      SparkFrameManager.concatenatePaths(conn.getStartPath, locationInfo.getPath)
    }
    frame.write.parquet(fullLocation)
  }
}
