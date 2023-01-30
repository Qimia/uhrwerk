package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.TableModel
import java.io.Serializable
import java.util.*

class TableResult : Serializable {
    var newResult: TableModel? = null
    var oldResult: TableModel? = null
    var isSuccess = false
    var isError = false
    var message: String? = null
    var exception: Exception? = null
    var targetResult: TargetResult? = null
    var sourceResults: Array<SourceResult>? = null
    var dependencyResult: DependencyStoreResult? = null
    override fun toString(): String {
        return ("TableResult{"
                + "success="
                + isSuccess
                + ", error="
                + isError
                + ", message='"
                + message
                + '\''
                + ", exception="
                + exception
                + ", targetResult="
                + targetResult
                + ", sourceResults="
                + Arrays.toString(sourceResults)
                + ", dependencyResult="
                + dependencyResult
                + '}')
    }

}