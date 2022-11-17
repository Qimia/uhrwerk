package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException

data class AWSSecret(
    override var name: String? = null,
    @JsonProperty("aws_secret_name")
    var awsSecretName: String? = null,
    @JsonProperty("aws_region")
    var awsRegion: String? = null
) : Secret() {
    override fun validate(path: String) {
        var path = path
        path += "aws/"
        if (this.awsSecretName == null) {
            throw ConfigException("Missing field: " + path + "aws_name")
        }
        if (this.awsRegion == null) {
            throw ConfigException("Missing field: " + path + "aws_region")
        }
    }
}