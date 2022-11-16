package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

@JsonDeserialize(using = ReferenceDeserializer::class)
data class Reference(
    var area: String? = null,
    var vertical: String? = null,
    var table: String? = null,
    var version: String? = null
)