package io.qimia.uhrwerk.config.representation

import java.util.LinkedHashMap


data class FunctionCall(
    var name: String? = null,
    var args: LinkedHashMap<String, String>? = null,
    var inputs: LinkedHashMap<String,String>? = null,
    var output: String? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionCall) return false

        if (name != other.name) return false
        if (args != other.args) return false
        if (inputs != other.inputs) return false
        return output == other.output
    }

    override fun hashCode(): Int {
        var result = name?.hashCode() ?: 0
        result = 31 * result + (args?.hashCode() ?: 0)
        result = 31 * result + (inputs?.hashCode() ?: 0)
        result = 31 * result + (output?.hashCode() ?: 0)
        return result
    }


}
