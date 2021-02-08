package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.engine.TableWrapper

import java.time.LocalDateTime
import scala.collection.mutable

/**
  * A single task to be put on a work queue
  * @param table Table config + code for running the config
  * @param partitions Which partitions belong to this single task (bulk-mode)
  * @param missingDependencies References to tasks which need to be completed before this can be done
  * @param upstreamDependencies After current task, update the dependencies of these other tasks
  */
class DT2(
    table: TableWrapper,
    parts: mutable.ListBuffer[LocalDateTime],
    missingDeps: mutable.Set[DT2Key],
    upstreamDeps: mutable.ListBuffer[DT2Key]
) {
  val tableWrapper = table
  val partitions = parts
  val missingDependencies = missingDeps
  val upstreamDependencies = upstreamDeps
}
