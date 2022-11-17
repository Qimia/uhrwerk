package io.qimia.uhrwerk.common.metastore.model

import java.sql.Timestamp
import java.time.LocalDateTime

data class SecretModel(
    var id: Long? = null,
    var name: String? = null,
    var type: SecretType? = null,
    var awsSecretName: String? = null,
    var awsRegion: String? = null,
    var description: String? = null,
    var deactivatedTs: LocalDateTime? = null,
    var createdTs: Timestamp? = null,
    var updatedTs: Timestamp? = null
) : BaseModel {
    override fun id(id: Long?) {
        this.id = id;
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SecretModel) return false
        if (id != null && other.id != null) if (id != other.id) return false
        if (name != other.name) return false
        if (type != other.type) return false
        if (awsSecretName != other.awsSecretName) return false
        if (awsRegion != other.awsRegion) return false
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
        result = 31 * result + (awsSecretName?.hashCode() ?: 0)
        result = 31 * result + (awsRegion?.hashCode() ?: 0)
        result = 31 * result + (description?.hashCode() ?: 0)
        result = 31 * result + (deactivatedTs?.hashCode() ?: 0)
        result = 31 * result + (createdTs?.hashCode() ?: 0)
        result = 31 * result + (updatedTs?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "SecretModel(id=$id, name=$name, type=$type, awsSecretName=$awsSecretName, awsRegion=$awsRegion, description=$description, deactivatedTs=$deactivatedTs, createdTs=$createdTs, updatedTs=$updatedTs)"
    }
}
