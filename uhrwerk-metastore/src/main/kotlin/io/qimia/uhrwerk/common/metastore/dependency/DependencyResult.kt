package io.qimia.uhrwerk.common.metastore.dependency

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.Partition
import java.io.Serializable
import java.time.LocalDateTime

data class DependencyResult(
    var partitionTs: LocalDateTime? = null,
    var isSuccess: Boolean = false,
    var dependency: DependencyModel? = null,
    var connection: ConnectionModel? = null,
    var succeeded: Array<LocalDateTime>? = null,
    var failed: Array<LocalDateTime>? = null,
    var partitions: Array<Partition>? = null
) : Serializable {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DependencyResult) return false
        if (partitionTs != other.partitionTs) return false
        if (isSuccess != other.isSuccess) return false
        if (dependency != other.dependency) return false
        if (connection != other.connection) return false
        if (succeeded != null) {
            if (other.succeeded == null) return false
            if (!succeeded.contentEquals(other.succeeded)) return false
        } else if (other.succeeded != null) return false
        if (failed != null) {
            if (other.failed == null) return false
            if (!failed.contentEquals(other.failed)) return false
        } else if (other.failed != null) return false
        if (partitions != null) {
            if (other.partitions == null) return false
            if (!partitions.contentEquals(other.partitions)) return false
        } else if (other.partitions != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = partitionTs?.hashCode() ?: 0
        result = 31 * result + isSuccess.hashCode()
        result = 31 * result + (dependency?.hashCode() ?: 0)
        result = 31 * result + (connection?.hashCode() ?: 0)
        result = 31 * result + (succeeded?.contentHashCode() ?: 0)
        result = 31 * result + (failed?.contentHashCode() ?: 0)
        result = 31 * result + (partitions?.contentHashCode() ?: 0)
        return result
    }
}
