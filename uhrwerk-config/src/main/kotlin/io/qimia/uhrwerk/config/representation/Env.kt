package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException
import java.util.*

data class Env(
    @JsonProperty("meta_store")
    var metastore: Metastore? = null
) {
    fun validate(path: String) {
        var path = path
        path += "env/"
        if (metastore == null) {
            throw ConfigException("Missing field: " + path + "metastore")
        }
    }
}