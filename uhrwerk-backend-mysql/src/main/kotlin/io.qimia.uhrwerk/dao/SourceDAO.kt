package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.ConnectionService
import io.qimia.uhrwerk.common.metastore.config.SourceResult
import io.qimia.uhrwerk.common.metastore.config.SourceService
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
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
            var conn: ConnectionModel? = null
            if (source.connection!!.name != null) {
                conn = connService.getByHashKey(HashKeyUtils.connectionKey(source.connection!!))
            }
            if (conn == null) {
                throw NullPointerException(
                    "The connection for this source is missing in the Metastore."
                )
            } else {
                //FIXME: issue with target connectionId
                source.connection = conn
                source.connectionId = conn.id
            }

            val oldSource: SourceModel2? = getByHashKey(source)
            result.oldResult = oldSource
            if (!overwrite) {
                if (oldSource != null) {
                    if (oldSource != source) {
                        result.message =
                            "A Source with id=${oldSource.id} and different values already exists in the Metastore."
                        result.isSuccess = false
                    } else {
                        result.isSuccess = true
                    }
                    return result
                }
            } else {
                if (oldSource != null)
                    repo.deactivateById(oldSource.id!!)
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

    private fun getByHashKey(source: SourceModel2): SourceModel2? =
        repo.getByHashKey(HashKeyUtils.sourceKey(source))

    override fun save(
        sources: List<SourceModel2>,
        overwrite: Boolean
    ) = sources.map { save(it, overwrite) }

    override fun getSourcesByTableId(tableId: Long): List<SourceModel2> {

        val sources = repo.getSourcesByTableId(tableId)
        if (!sources.isNullOrEmpty()) {
            sources.forEach {
                val conn = connService.getById(it.connectionId)
                it.connection = conn
            }
        }
        return sources
    }
}