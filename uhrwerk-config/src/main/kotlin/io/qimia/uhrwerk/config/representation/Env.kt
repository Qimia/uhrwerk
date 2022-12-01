package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException
import java.util.*

data class Env(
    var secrets: Array<Secret>? = null,
    @JsonProperty("meta_store")
    var metastore: Metastore? = null
) {
    fun validate(path: String) {
        var path = path
        path += "env/"
        if (metastore == null) {
            throw ConfigException("Missing field: " + path + "metastore")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Env) return false

        if (secrets != null) {
            if (other.secrets == null) return false
            if (!secrets.contentEquals(other.secrets)) return false
        } else if (other.secrets != null) return false
        if (metastore != other.metastore) return false

        return true
    }

    override fun hashCode(): Int {
        var result = secrets?.contentHashCode() ?: 0
        result = 31 * result + (metastore?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Env(secrets=${secrets?.contentToString()}, metastore=$metastore)"
    }
}