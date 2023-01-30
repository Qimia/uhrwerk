package io.qimia.uhrwerk.common.metastore.config

import java.io.Serializable

class PartitionDependencyResult : Serializable {
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null

    companion object {
        private const val serialVersionUID = -8972593554921481622L
    }
}