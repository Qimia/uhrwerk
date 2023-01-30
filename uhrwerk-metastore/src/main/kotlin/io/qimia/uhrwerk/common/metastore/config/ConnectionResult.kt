package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import java.io.Serializable
import java.util.*

class ConnectionResult : Serializable {
    var newConnection: ConnectionModel? = null
    var oldConnection: ConnectionModel? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false
        val that = o as ConnectionResult
        return isSuccess == that.isSuccess && isError == that.isError && newConnection == that.newConnection && oldConnection == that.oldConnection && message == that.message && exception == that.exception
    }

    override fun hashCode(): Int {
        return Objects.hash(newConnection, oldConnection, isSuccess, isError, message, exception)
    }

    override fun toString(): String {
        return ("ConnectionResult{"
                + "newConnection="
                + newConnection
                + ", oldConnection="
                + oldConnection
                + ", success="
                + isSuccess
                + ", error="
                + isError
                + ", message='"
                + message
                + '\''
                + ", exception="
                + exception
                + '}')
    }

}