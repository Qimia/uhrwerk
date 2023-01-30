package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.model.TargetModel
import java.io.Serializable

class TargetResult : Serializable {
    var storedTargets: Array<TargetModel>? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
    override fun toString(): String {
        return ("TargetResult{"
                + "success="
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

    companion object {
        private const val serialVersionUID = 740323809677539485L
    }
}