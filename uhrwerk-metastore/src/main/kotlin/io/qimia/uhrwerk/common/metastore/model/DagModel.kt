package io.qimia.uhrwerk.common.metastore.model

data class DagModel(
    var secrets: Array<SecretModel>? = null,
    var connections: Array<ConnectionModel>? = null,
    var tables: Array<TableModel>? = null
)