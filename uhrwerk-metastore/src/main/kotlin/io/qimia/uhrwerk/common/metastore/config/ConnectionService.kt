package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import java.sql.SQLException

interface ConnectionService {
    fun save(Connection: ConnectionModel, overwrite: Boolean): ConnectionResult

    @Throws(SQLException::class)
    fun getById(id: Long): ConnectionModel?

    @Throws(SQLException::class)
    fun getByHashKey(id: Long): ConnectionModel?

    /**
     * Get All the Dependency Connections for a given Table
     *
     * @param tableId
     * @return Connections belonging to the table's dependencies
     * @throws SQLException
     */
    @Throws(SQLException::class)
    fun getAllTableDeps(tableId: Long): List<ConnectionModel>
}