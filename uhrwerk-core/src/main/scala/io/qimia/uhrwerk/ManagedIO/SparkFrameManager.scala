package io.qimia.uhrwerk.ManagedIO
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkFrameManager {
  // Convert batch start timestamp to postfix needed for reading single batch
  def dateTimeToPostFix(date: LocalDateTime, duration: Duration): String = {
    val year = date.getYear
    val month = date.getMonth.getValue
    val day = date.getDayOfMonth
    if (duration.toHours >= Duration.ofDays(1).toHours) {
      return s"/year=${year}/month=${year}-${month}/day=${year}-${month}-${day}"
    }
    val hour = date.getHour
    if (duration.toMinutes >= Duration.ofHours(1).toMinutes) {
      return s"/year=${year}/month=${year}-${month}/day=${year}-${month}-${day}/hour=${hour}"
    }
    val batch = TimeTools.getBatchInHourNumber(date.toLocalTime, duration)
    s"/year=${year}/month=${year}-${month}/day=${year}-${month}-${day}/hour=${hour}/batch=${batch}"
  }
}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {

  // TODO: This more of a rough sketch than the actual full FrameManager. We will need to handle different batch sizes

  override def loadDFFromLake(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    // TODO: Make trailing slashes handling better
    // TODO: Needs to be able to load multiple batches as one dataframe efficiently
    val fullLocation = if(startTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateTimeToPostFix(startTS.get, duration)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    sparkSession.read.parquet(fullLocation)
  }

  override def writeDFToLake(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if(startTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateTimeToPostFix(startTS.get, duration)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    frame.write.parquet(fullLocation)
  }
}
