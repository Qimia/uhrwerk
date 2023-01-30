package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.SecretModel
import java.io.Serializable
import java.util.*

class SecretResult : Serializable {
    var newSecret: SecretModel? = null
    var oldSecret: SecretModel? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
}