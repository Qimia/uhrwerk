package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException

data class JDBC(
    override var name: String? = null,
    @JsonProperty("jdbc_url")
    var jdbcUrl: String? = null,
    @JsonProperty("jdbc_driver")
    var jdbcDriver: String? = null,
    var user: String? = null,
    var password: String? = null
) : Connection() {
    override fun validate(path: String) {
        var path = path
        //super.validate(path)
        path += "jdbc/"
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