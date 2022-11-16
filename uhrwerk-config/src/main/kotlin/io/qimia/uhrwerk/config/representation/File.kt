package io.qimia.uhrwerk.config.representation

import io.qimia.uhrwerk.config.builders.ConfigException

data class File(
    override var name: String? =null,
    var path: String? = null
) : Connection() {
    override fun validate(path: String) {
        //super.validate(path)
        if (this.path == null) {
            throw ConfigException("Missing field: " + path + "path")
        }
    }
}