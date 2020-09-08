package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.engine.Environment.Ident
import org.apache.spark.sql.DataFrame

case class TaskInput(inputFrames: Map[Ident, DataFrame])
