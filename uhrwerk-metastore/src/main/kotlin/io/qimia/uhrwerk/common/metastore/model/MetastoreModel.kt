package io.qimia.uhrwerk.common.metastore.model

data class MetastoreModel(
    var jdbc_url: String? = null,
    var jdbc_driver: String? = null,
    var user: String? = null,
    var pass: String? = null
)
