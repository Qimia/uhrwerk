package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class Dag(
    var secrets: Array<Secret>? = null,
    var connections: Array<Connection>? = null,
    var tables: Array<Table>? = null
) {
    fun validate(path: String) {
        var path = path
        path += "/"
        if (connections == null) {
            throw ConfigException("Missing field:" + path + "connections")
        } else {
            for (c in connections!!) {
                c.validate(path)
            }
        }
        if (tables == null) {
            throw ConfigException("Missing field:" + path + "tables")
        } else {
            for (t in tables!!) {
                t.validate(path)
            }
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Dag) return false

        if (secrets != null) {
            if (other.secrets == null) return false
            if (!secrets.contentEquals(other.secrets)) return false
        } else if (other.secrets != null) return false
        if (connections != null) {
            if (other.connections == null) return false
            if (!connections.contentEquals(other.connections)) return false
        } else if (other.connections != null) return false
        if (tables != null) {
            if (other.tables == null) return false
            if (!tables.contentEquals(other.tables)) return false
        } else if (other.tables != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = secrets?.contentHashCode() ?: 0
        result = 31 * result + (connections?.contentHashCode() ?: 0)
        result = 31 * result + (tables?.contentHashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Dag(secrets=${secrets?.contentToString()}, connections=${connections?.contentToString()}, tables=${tables?.contentToString()})"
    }
}