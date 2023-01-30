package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import java.io.Serializable

class DependencyStoreResult : Serializable {
    var dependenciesSaved: Array<DependencyModel>? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null

    override fun toString(): String {
        return ("DependencyStoreResult{"
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

}