package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException

data class S3(
    override var name: String? = null,
    var path: String? = null,
    @JsonProperty("secret_id")
    var secretId: String? = null,
    @JsonProperty("secret_key")
    var secretKey: String? = null
) : Connection() {
    override fun validate(path: String) {
        var path = path
        //super.validate(path)
        path += "s3/"
        if (this.path == null) {
            throw ConfigException("Missing field: " + path + "path")
        }
    }
}