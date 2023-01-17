package io.qimia.uhrwerk.tools

import io.qimia.uhrwerk.config.representation.Reference

data class TableInfo(
    val ref: Reference,
    var depth: Int = 0,
    var processed: Boolean = false,
    var exists: Boolean = false
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TableInfo) return false
        if (ref != other.ref) return false
        return true
    }

    override fun hashCode(): Int {
        return ref.hashCode()
    }

    override fun toString(): String {
        return "$ref, depth=$depth, processed=$processed, exists=$exists"
    }

    fun toStringWithIndent(): String {
        val indents = List<String>(depth) { _ -> "----" }
        val prefix = java.lang.String.join("", indents)
        return "+$prefix $this"
    }
}
