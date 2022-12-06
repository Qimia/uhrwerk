package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonRootName
import io.qimia.uhrwerk.config.builders.ConfigException

@JsonRootName("meta_store")
data class Metastore(
    @JsonProperty("env_name")
    var envName: String? = null,
    @JsonProperty("jdbc_url")
    var jdbcUrl: String? = null,
    @JsonProperty("jdbc_driver")
    var jdbcDriver: String? = null,
    var user: String? = null,
    var password: String? = null,
) {
    fun validate(path: String) {
        var path = path
        path += "metastore/"
        if (jdbcUrl == null) {
            throw ConfigException("Missing field: " + path + "jdbc_url")
        }
        if (user == null) {
            throw ConfigException("Missing field: " + path + "user")
        }
        if (password == null) {
            throw ConfigException("Missing field: " + path + "pass")
        }
    }
}