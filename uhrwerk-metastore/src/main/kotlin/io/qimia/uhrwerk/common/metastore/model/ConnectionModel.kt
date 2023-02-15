package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class ConnectionModel(
    var id: Long? = null,
    var name: String? = null,
    var type: ConnectionType? = null,
    var path: String? = null,
    var jdbcUrl: String? = null,
    var jdbcDriver: String? = null,
    var jdbcUser: String? = null,
    var jdbcPass: String? = null,
    var awsAccessKeyID: String? = null,
    var awsSecretAccessKey: String? = null,
    var redshiftFormat: String? = null,
    var redshiftAwsIamRole: String? = null,
    var redshiftTempDir: String? = null,
    var description: String? = null,
    var hashKey: Long? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ConnectionModel) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (name != other.name) return false
        if (type != other.type) return false
        if (path != other.path) return false
        if (jdbcUrl != other.jdbcUrl) return false
        if (jdbcDriver != other.jdbcDriver) return false
        if (jdbcUser != other.jdbcUser) return false
        if (jdbcPass != other.jdbcPass) return false
        if (awsAccessKeyID != other.awsAccessKeyID) return false
        if (awsSecretAccessKey != other.awsSecretAccessKey) return false
        if (redshiftFormat != other.redshiftFormat) return false
        if (redshiftAwsIamRole != other.redshiftAwsIamRole) return false
        if (redshiftTempDir != other.redshiftTempDir) return false
        if (hashKey != null && other.hashKey != null) if (hashKey != other.hashKey) return false
        if (description != other.description) return false
        if (deactivatedTs != other.deactivatedTs) return false
        if (createdTs != other.createdTs) return false
        if (updatedTs != other.updatedTs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = id?.hashCode() ?: 0
        result = 31 * result + (name?.hashCode() ?: 0)
        result = 31 * result + (type?.hashCode() ?: 0)
        result = 31 * result + (path?.hashCode() ?: 0)
        result = 31 * result + (jdbcUrl?.hashCode() ?: 0)
        result = 31 * result + (jdbcDriver?.hashCode() ?: 0)
        result = 31 * result + (jdbcUser?.hashCode() ?: 0)
        result = 31 * result + (jdbcPass?.hashCode() ?: 0)
        result = 31 * result + (awsAccessKeyID?.hashCode() ?: 0)
        result = 31 * result + (awsSecretAccessKey?.hashCode() ?: 0)
        result = 31 * result + (redshiftFormat?.hashCode() ?: 0)
        result = 31 * result + (redshiftAwsIamRole?.hashCode() ?: 0)
        result = 31 * result + (redshiftTempDir?.hashCode() ?: 0)
        result = 31 * result + (hashKey?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }


}