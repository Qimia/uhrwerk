package io.qimia.uhrwerk.common.metastore.dependency

import java.io.Serializable
import java.time.LocalDateTime

class TablePartitionResultSet(
    var resolvedTs: Array<LocalDateTime>? = null,
    var processedTs: Array<LocalDateTime>? = null,
    var failedTs: Array<LocalDateTime>? = null,
    var resolved: Array<TablePartitionResult>? = null,
    var processed: Array<TablePartitionResult>? = null,
    var failed: Array<TablePartitionResult>? = null
) : Serializable