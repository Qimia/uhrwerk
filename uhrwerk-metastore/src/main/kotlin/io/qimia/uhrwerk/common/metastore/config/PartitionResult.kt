package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.Partition
import java.io.Serializable

class PartitionResult : Serializable {
    var newResult: Partition? = null
    var oldResult: Partition? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
}