package io.qimia.uhrwerk.common.metastore.model

data class DagModel(
    var connections: Array<ConnectionModel>? = null,
    var tables: Array<TableModel>? = null
)