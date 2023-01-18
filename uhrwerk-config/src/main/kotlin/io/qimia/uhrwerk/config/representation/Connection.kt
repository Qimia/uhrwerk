package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import io.qimia.uhrwerk.config.builders.ConfigException

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    Type(value = JDBC::class, name = "jdbc"),
    Type(value = S3::class, name = "s3"),
    Type(value = File::class, name = "file"),
    Type(value = Redshift::class, name = "redshift")
)
open class Connection(
    open var name: String? = null
) {
    open fun validate(path: String) {
        var path = path
        path += "connection/"
        if (name == null) {
            throw ConfigException("Missing field: " + path + "name")
        }
    }
}