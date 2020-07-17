package io.qimia.uhrwerk.ManagedIO
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkFrameManager {

  @scala.annotation.tailrec
  def leftPad(s: String): String = {
    if (s.length < 2) {
      leftPad("0" + s)
    } else {
      s
    }
  }

  // Convert batch start timestamp to postfix needed for reading single batch
  def dateTimeToPostFix(date: LocalDateTime, duration: Duration): String = {
    val year = date.getYear
    val month = leftPad(date.getMonthValue.toString)
    val day = leftPad(date.getDayOfMonth.toString)
    if (duration.toHours >= Duration.ofDays(1).toHours) {
      return s"${year}-${month}-${day}"
    }
    val hour = leftPad(date.getHour.toString)
    if (duration.toMinutes >= Duration.ofHours(1).toMinutes) {
      return s"${year}-${month}-${day}-${hour}"
    }
    val batch = TimeTools.getBatchInHourNumber(date.toLocalTime, duration).toString
    s"${year}-${month}-${day}-${hour}-${batch}"
  }
}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {

  // TODO: This more of a rough sketch than the actual full FrameManager. We will need to handle different batch sizes

  // Load dataframe from datalake
  override def loadDFFromLake(conn: Connection,
                              locationInfo: Dependency,
                              startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    // TODO: Make trailing slashes handling better
    // TODO: Needs to be able to load multiple batches as one dataframe efficiently
    val fullLocation = if (startTS.isDefined) {
      val duration =
        TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + s"/batch=${SparkFrameManager
        .dateTimeToPostFix(startTS.get, duration)}"
    } else {
      conn.getStartPath + locationInfo.getPath
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
    val loc = conn.getStartPath + locationInfo.getPath
    val df = sparkSession.read.parquet(loc)
    val startRange = SparkFrameManager
      .dateTimeToPostFix(startTS, batchDuration)
    val endRange = SparkFrameManager
      .dateTimeToPostFix(endTSExcl, batchDuration)
    df.where(($"batch" >= startRange) and ($"batch" < endRange))
  }

  // safe dataframe to datalake
  override def writeDFToLake(frame: DataFrame,
                             conn: Connection,
                             locationInfo: Target,
                             startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if (startTS.isDefined) {
      val duration =
        TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + s"/batch=${SparkFrameManager
        .dateTimeToPostFix(startTS.get, duration)}"
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    frame.write.parquet(fullLocation)
  }
}
