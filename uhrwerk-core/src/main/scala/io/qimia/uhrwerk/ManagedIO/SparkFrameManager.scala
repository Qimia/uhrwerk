package io.qimia.uhrwerk.ManagedIO
import java.time.{Duration, LocalDateTime}

import io.qimia.uhrwerk.models.config.{Connection, Dependency, Target}
import io.qimia.uhrwerk.utils.TimeTools
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkFrameManager {
  // Convert batch start timestamp to postfix needed for reading single batch
  def dateToPostFix(date: LocalDateTime, duration: Duration): String = {
    val year = date.getYear
    val month = date.getMonth
    val day = date.getDayOfMonth
    val hour = date.getHour
    // TODO Different postfix for a different duration
    s"/year=${year}/month=${year}-${month}/day=${year}-${month}-${day}/hour=${hour}"
  }
}

class SparkFrameManager(sparkSession: SparkSession) extends FrameManager {

  // TODO: This more of a rough sketch than the actual function. We will need to handle different batch sizes

  override def loadDFFromLake(conn: Connection, locationInfo: Dependency, startTS: Option[LocalDateTime]): DataFrame = {
    assert(conn.getName == locationInfo.getConnectionName)
    // TODO: Make trailing slashes handling better
    val fullLocation = if(startTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateToPostFix(startTS.get, duration)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    sparkSession.read.parquet(fullLocation)
  }

  override def writeDFToLake(frame: DataFrame, conn: Connection, locationInfo: Target, startTS: Option[LocalDateTime]): Unit = {
    assert(conn.getName == locationInfo.getConnectionName)
    val fullLocation = if(startTS.isDefined) {
      val duration = TimeTools.convertDurationToObj(locationInfo.getPartitionSize)
      conn.getStartPath + locationInfo.getPath + SparkFrameManager.dateToPostFix(startTS.get, duration)
    } else {
      conn.getStartPath + locationInfo.getPath
    }
    frame.write.parquet(fullLocation)
  }
}
