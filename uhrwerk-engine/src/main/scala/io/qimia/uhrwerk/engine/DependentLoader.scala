package io.qimia.uhrwerk.engine

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.framemanager.FrameManager
import io.qimia.uhrwerk.common.model.Source
import org.apache.spark.sql.DataFrame

/**
  * Special class that allows for the retrieval of dataframes which depends on other dataframes
  */
class DependentLoader(source: Source,
                      startTS: Option[LocalDateTime] = Option.empty,
                      endTSExcl: Option[LocalDateTime] = Option.empty,
                      dataFrameReaderOptions: Option[Map[String, String]] = Option.empty,
                      frameManager: FrameManager) {
  var dependentSet: Boolean = false

  def addDependentFrame(df: DataFrame, columnName: String, queryName: String): Unit = {
    dependentSet = true
  }

  def getFrame: DataFrame = {
    if (!dependentSet) {
      frameManager.loadSourceDataFrame(source, startTS, endTSExcl)
    } else {
      // TODO: Need interface and method for returning
      null
    }
  }

}
