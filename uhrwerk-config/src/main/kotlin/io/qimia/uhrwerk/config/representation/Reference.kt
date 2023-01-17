package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

@JsonDeserialize(using = ReferenceDeserializer::class)
data class Reference(
    var area: String? = null,
    var vertical: String? = null,
    var table: String? = null,
    var version: String? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Reference) return false

        if (area != other.area) return false
        if (vertical != other.vertical) return false
        if (table != other.table) return false
        if (version != other.version) return false

        return true
    }

    override fun hashCode(): Int {
        var result = area?.hashCode() ?: 0
        result = 31 * result + (vertical?.hashCode() ?: 0)
        result = 31 * result + (table?.hashCode() ?: 0)
        result = 31 * result + (version?.hashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "$area.$vertical.$table:$version"
    }
}