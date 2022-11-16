package io.qimia.uhrwerk.common.framemanager

import io.qimia.uhrwerk.common.metastore.model.{ConnectionModel, DependencyModel, Partition}

import java.time.LocalDateTime

case class BulkDependencyResult(
                                 partitionTimestamps: Array[LocalDateTime],
                                 dependency: DependencyModel,
                                 connection: ConnectionModel,
                                 succeeded: Array[Partition]
                               )