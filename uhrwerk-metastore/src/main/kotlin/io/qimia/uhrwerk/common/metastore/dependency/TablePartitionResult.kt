package io.qimia.uhrwerk.common.metastore.dependency

import java.io.Serializable
import java.time.LocalDateTime

class TablePartitionResult(
    var partitionTs: LocalDateTime? = null,
    var isProcessed: Boolean = false,
    var isResolved: Boolean = false,
    var resolvedDependencies: Array<DependencyResult>? = null,
    var failedDependencies: Array<DependencyResult>? = null
) : Serializable

