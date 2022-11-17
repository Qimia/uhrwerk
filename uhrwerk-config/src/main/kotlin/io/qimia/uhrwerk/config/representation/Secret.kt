package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.qimia.uhrwerk.config.builders.ConfigException

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = AWSSecret::class, name = "aws")
)
open class Secret(
    open var name: String? = null
) {
    open fun validate(path: String) {
        var path = path
        path += "secret/"
        if (name == null) {
            throw ConfigException("Missing field: " + path + "name")
        }
    }
}