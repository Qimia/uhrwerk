package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonNode
import java.io.IOException

class ReferenceDeserializer : JsonDeserializer<Reference>() {
    @Throws(IOException::class, JacksonException::class)
    override fun deserialize(parser: JsonParser?, context: DeserializationContext?): Reference? {
        val node = parser!!.codec.readTree<JsonNode>(parser)

        var area: String? = null
        var vertical: String? = null
        var table: String? = null
        var version: String? = null

        val hasRef = node.isTextual

        if (hasRef) {
            val reference = node.asText()

            if (reference != null && reference.isNotEmpty()) {
                val groups = reference.split(":")
                val groupsNotEmpty = groups.all { it.isNotEmpty() }

                if (groupsNotEmpty && groups.size == 2) {
                    val names = groups[0].split(".")
                    val namesNotEmpty = names.all { it.isNotEmpty() }

                    if (namesNotEmpty && names.size == 3) {
                        area = names[0]
                        vertical = names[1]
                        table = names[2]
                    }

                    version = groups[1]
                }
            }
        } else {
            area = node.get("area").asText();
            vertical = node.get("vertical").asText();
            table = node.get("table").asText();
            version = node.get("version").asText();
        }

        val refSet = listOf(area, vertical, table, version).all { it != null }
        if (refSet)
            return Reference(area, vertical, table, version)

        return null
    }
}