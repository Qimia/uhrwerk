package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException
import java.util.*

class Target(
    @JsonProperty("connection_name")
    var connectionName: String? = null,
    var format: String? = null
) {
    fun validate(path: String) {
        var path = path
        path += "target/"
        if (connectionName == null) {
            throw ConfigException("Missing field: " + path + "connection_name")
        }
        if (format == null) {
            throw ConfigException("Missing field: " + path + "format")
        }
        if (!Arrays.asList("json", "parquet", "jdbc", "orc", "libsvm", "csv", "text", "avro")
                .contains(format)
        ) {
            throw ConfigException("Wrong format! '" + format + "' is not allowed in " + path + "format")
        }
    }
}