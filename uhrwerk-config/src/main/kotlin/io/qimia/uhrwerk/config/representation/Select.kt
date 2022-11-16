package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class Select(
    var query: String? = null,
    var column: String? = null
) {

    fun validate(path: String) {
        var path = path
        path += "select/"
        if (query == null) {
            throw ConfigException("Missing field: " + path + "query")
        }
        if (column == null) {
            throw ConfigException("Missing field: " + path + "column")
        }
    }

    fun validateUnpartitioned(path: String) {
        var path = path
        path += "select/"
        if (query == null) {
            throw ConfigException("Missing field: " + path + "query")
        }
    }
}