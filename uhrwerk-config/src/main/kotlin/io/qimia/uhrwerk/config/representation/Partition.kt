package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class Partition(
    var unit: PartitionUnit? = null,
    var size: Int? = null
) {
    fun validate(path: String) {
        var path = path
        path += "partition/"
        if (unit == null) {
            throw ConfigException("Missing field: " + path + "unit")
        }
        if (!PartitionUnit.values().contains(unit)) {
            throw ConfigException("Wrong unit! '" + unit + "' is not allowed in " + path + "unit")
        }
        if (size == null) {
            throw ConfigException("Missing field: " + path + "size")
        }
    }

    fun validate(path: String, type: String) {
        var path = path
        path += "partition/"
        if (type == "temporal_aggregate") {
            if (unit == null) {
                throw ConfigException("Missing field: " + path + "unit")
            }
            if (!PartitionUnit.values().contains(unit!!)) {
                throw ConfigException("Wrong unit! '" + unit + "' is not allowed in " + path + "unit")
            }
        }
        if (size == 0) {
            throw ConfigException("Missing field: " + path + "size")
        }
    }
}