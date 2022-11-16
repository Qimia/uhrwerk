package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class Transform(

    var type: TransformType? = TransformType.NONE,
    var partition: Partition? = null
) {

    fun validate(path: String) {
        var path = path
        path += "transform/"
        if (type == null) {
            throw ConfigException("Missing field: " + path + "type")
        }
        if (!TransformType.values().contains(type)) {
            throw ConfigException("Wrong type! '" + type + "' is not allowed in " + path + "type")
        }
        if (type != TransformType.IDENTITY) {
            if (partition == null) {
                throw ConfigException("Missing field: " + path + "partition")
            } else {
                partition!!.validate(path, type!!.name)
            }
        }
    }

}