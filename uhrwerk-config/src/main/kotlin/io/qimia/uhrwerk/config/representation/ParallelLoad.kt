package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class ParallelLoad(
    var query: String? = null,
    var column: String? = null,
    var num: Int? = null
) {
    fun validate(path: String) {
        var path = path
        path += "parallel_load/"
        if (query == null) {
            throw ConfigException("Missing field: " + path + "query")
        }
        if (column == null) {
            throw ConfigException("Missing field: " + path + "column")
        }
        if (num == null) {
            throw ConfigException("Missing field: " + path + "num")
        }
    }
}