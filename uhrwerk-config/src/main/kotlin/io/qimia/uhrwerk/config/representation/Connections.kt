package io.qimia.uhrwerk.config.representation

data class Connections(
    var secrets: Array<Secret>? = null,
    var connections: Array<Connection>? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Connections) return false
        if (secrets != null) {
            if (other.secrets == null) return false
            if (!secrets.contentEquals(other.secrets)) return false
        } else if (other.secrets != null) return false
        if (connections != null) {
            if (other.connections == null) return false
            if (!connections.contentEquals(other.connections)) return false
        } else if (other.connections != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = secrets?.contentHashCode() ?: 0
        result = 31 * result + (connections?.contentHashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Connections(secrets=${secrets?.contentToString()}, connections=${connections?.contentToString()})"
    }
}
