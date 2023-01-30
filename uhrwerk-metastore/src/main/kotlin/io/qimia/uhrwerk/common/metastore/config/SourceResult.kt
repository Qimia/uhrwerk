package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.SourceModel2
import java.io.Serializable
import java.util.*

class SourceResult : Serializable {
    var newResult: SourceModel2? = null
    var oldResult: SourceModel2? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
}