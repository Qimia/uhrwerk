package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException
import java.util.*
@JsonIgnoreProperties(ignoreUnknown = true)
data class Dependency(
    @JsonProperty("ref")
    var reference: Reference? = null,
    var view: String? = null,
    var format: String? = null,
) {
    fun validate(path: String) {
        var path = path
        path += "dependency/"
        if (reference?.area == null) {
            throw ConfigException("Missing field: " + path + "area")
        }
        if (reference?.vertical == null) {
            throw ConfigException("Missing field: " + path + "vertical")
        }
        if (reference?.table == null) {
            throw ConfigException("Missing field: " + path + "table")
        }
        if (format == null) {
            throw ConfigException("Missing field: " + path + "format")
        }
        if (!Arrays.asList("json", "parquet", "jdbc", "orc", "libsvm", "csv", "text", "avro")
                .contains(format)
        ) {
            throw ConfigException("Wrong format! '" + format + "' is not allowed in " + path + "format")
        }
        if (reference?.version == null) {
            throw ConfigException("Missing field: " + path + "version")
        }
    }
}