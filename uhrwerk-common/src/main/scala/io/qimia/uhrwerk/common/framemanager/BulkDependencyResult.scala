package io.qimia.uhrwerk.common.framemanager

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.model.{ConnectionModel, DependencyModel, Partition}

case class BulkDependencyResult(
                                 partitionTimestamps: Array[LocalDateTime],
                                 dependency: DependencyModel,
                                 connection: ConnectionModel,
                                 succeeded: Array[Partition]
                               )