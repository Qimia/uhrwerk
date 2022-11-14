package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.ConnectionResult
import io.qimia.uhrwerk.common.metastore.config.ConnectionService
import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.HashKeyUtils
import io.qimia.uhrwerk.repo.ConnectionRepo
import java.sql.SQLException

class ConnectionDAO : ConnectionService {
    private val repo = ConnectionRepo()

    override fun getByHashKey(hashKey: Long): ConnectionModel? {
        val conns = repo.getByHashKey(hashKey)
        if (conns.isNotEmpty())
            return conns.first()
        return null
    }

    override fun getById(id: Long): ConnectionModel? {
        return repo.getById(id)
    }

    override fun getAllTableDeps(tableId: Long): List<ConnectionModel> {
        return repo.getAllTableDeps(tableId)
    }


    override fun save(connection: ConnectionModel, overwrite: Boolean): ConnectionResult {
        val result = ConnectionResult()
        result.oldConnection = getByHashKey(HashKeyUtils.connectionKey(connection))
        try {
            if (!overwrite && result.oldConnection != null) {
                result.message = String.format(
                    "A Connection with name=%s already exists in the Metastore.",
                    connection.name
                )
                return result
            }
            if (result.oldConnection != null)
                repo.deactivateById(result.oldConnection.id)

            result.newConnection = repo.save(connection)
            result.isSuccess = true

        } catch (e: SQLException) {
            result.isError = true
            result.isSuccess = false
            result.exception = e
            result.message = e.message
        }
        return result
    }

}