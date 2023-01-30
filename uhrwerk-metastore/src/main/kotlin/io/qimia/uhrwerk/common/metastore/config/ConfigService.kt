package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.TableModel

interface ConfigService {
    fun save(Connection: ConnectionModel?): ConnectionModel?
    fun tableConfig(area: String?, vertical: String?, name: String?, version: String?): TableModel?
    fun tableConfig(table: TableModel?): TableModel?
}