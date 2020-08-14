package io.qimia.uhrwerk.common.framemanager

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{Connection, Dependency, Partition}

case class BulkDependencyResult(
                                 partitionTimestamps: Array[LocalDateTime],
                                 dependency: Dependency,
                                 connection: Connection,
                                 succeeded: Array[Partition]
                               )