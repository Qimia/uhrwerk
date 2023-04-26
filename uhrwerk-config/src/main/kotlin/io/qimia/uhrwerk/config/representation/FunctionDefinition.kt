package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty

data class FunctionDefinition(
    var name: String? = null,
    @JsonProperty("sql_query")
    var sqlQuery: String? = null,
    @JsonProperty("class_name")
    var className: String? = null,
    var params: Array<String>? = null,
    @JsonProperty("input_views")
    var inputViews: Array<String>? = null,
    var output: String? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionDefinition) return false

        if (name != other.name) return false
        if (sqlQuery != other.sqlQuery) return false
        if (className != other.className) return false
        if (params != null) {
            if (other.params == null) return false
            if (!params.contentEquals(other.params)) return false
        } else if (other.params != null) return false

        if (inputViews != null) {
            if (other.inputViews == null) return false
            if (!inputViews.contentEquals(other.inputViews)) return false
        } else if (other.inputViews != null) return false

        return output == other.output
    }

    override fun hashCode(): Int {
        var result = name?.hashCode() ?: 0
        result = 31 * result + (sqlQuery?.hashCode() ?: 0)
        result = 31 * result + (className?.hashCode() ?: 0)
        result = 31 * result + (params?.contentHashCode() ?: 0)
        result = 31 * result + (inputViews?.contentHashCode() ?: 0)
        result = 31 * result + (output?.hashCode() ?: 0)
        return result
    }
}
