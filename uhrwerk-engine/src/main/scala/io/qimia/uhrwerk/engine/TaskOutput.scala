package io.qimia.uhrwerk.engine

import org.apache.spark.sql.DataFrame

case class TaskOutput(frame: DataFrame, dataFrameWriterOptions: Option[Array[Map[String, String]]] = Option.empty)
