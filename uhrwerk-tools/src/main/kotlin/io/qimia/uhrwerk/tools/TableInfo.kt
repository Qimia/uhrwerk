package io.qimia.uhrwerk.tools

import io.qimia.uhrwerk.config.representation.Reference

data class TableInfo(
    val ref: Reference,
    var process: Boolean = false,
    var exists: Boolean = false
)
