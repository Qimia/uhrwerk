package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.ConnectionService
import io.qimia.uhrwerk.common.metastore.config.SourceResult
import io.qimia.uhrwerk.common.metastore.config.SourceService
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.SourceModel2
import io.qimia.uhrwerk.repo.SourceRepo2
import java.sql.SQLException

class SourceDAO : SourceService {
    private val connService: ConnectionService = ConnectionDAO()
    private val repo = SourceRepo2()


    override fun save(source: SourceModel2, overwrite: Boolean): SourceResult {
        val result = SourceResult()
        result.newResult = source

        try {
            if (source.connection == null) {
                throw NullPointerException(
                    "The connection in this source is null. It needs to be set."
                )
            }
            val connectionKey = HashKeyUtils.connectionKey(source.connection!!)
            val connection = connService.getByHashKey(connectionKey)
            if (connection == null) {
                throw NullPointerException(
                    "The connection for this source is missing in the Metastore."
                )
            } else {
                source.connection = connection
                source.connectionKey = connectionKey
            }

            val sourceKey = HashKeyUtils.sourceKey(source)
            val oldSource = repo.getByHashKey(sourceKey)
            result.oldResult = oldSource
            if (!overwrite) {
                if (oldSource != null) {
                    if (oldSource != source) {
                        result.message =
                            "A Source with hash_key=$sourceKey and different values already exists in the Metastore."
                        result.isSuccess = false
                    } else {
                        result.isSuccess = true
                    }
                    return result
                }
            }
            result.newResult = repo.save(source)
            result.isSuccess = true
        } catch (e: SQLException) {
            result.isError = true
            result.exception = e
            result.message = e.message
        } catch (e: NullPointerException) {
            result.isError = true
            result.exception = e
            result.message = e.message
        }
        return result
    }

    override fun save(
        sources: List<SourceModel2>,
        overwrite: Boolean
    ) = sources.map { save(it, overwrite) }

    override fun getByTableId(tableId: Long): List<SourceModel2> {

        val sources = repo.getByTableId(tableId)
        if (!sources.isNullOrEmpty()) {
            sources.forEach {
                val conn = connService.getByHashKey(it.connectionKey!!)
                it.connection = conn
            }
        }
        return sources
    }

    override fun deactivateByTableKey(tableKey: Long): Int? {
        return repo.deactivateByTableKey(tableKey)
    }
}