package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.model.ConnectionModel
import io.qimia.uhrwerk.common.model.ConnectionType
import io.qimia.uhrwerk.common.model.HashKeyUtils
import java.sql.PreparedStatement
import java.sql.ResultSet

class ConnectionRepo : BaseRepo<ConnectionModel>() {
    fun save(connection: ConnectionModel): ConnectionModel? =
        super.insert(connection, INSERT) {
            insertParams(connection, it)
        }

    fun getByHashKey(hashKey: Long): List<ConnectionModel> =
        super.findAll(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun getById(id: Long): ConnectionModel? =
        super.find(
            SELECT_BY_ID, {
                it.setLong(1, id)
            }, this::map
        )

    fun deactivateById(id: Long): Int? =
        super.update(
            DEACTIVATE_BY_ID,
        ) {
            it.setLong(1, id)
        }


    fun getAllTableDeps(tableId: Long): List<ConnectionModel> =
        super.findAll(
            SELECT_ALL_TABLE_DEPS, {
                it.setLong(1, tableId)
            }, this::map
        )

    private fun insertParams(
        entity: ConnectionModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setString(1, entity.name)
        insert.setString(2, entity.type.name)
        insert.setString(3, entity.path)
        // jdbc columns values
        insert.setString(4, entity.jdbcUrl)
        insert.setString(5, entity.jdbcDriver)
        insert.setString(6, entity.jdbcUser)
        insert.setString(7, entity.jdbcPass)
        // aws columns values
        insert.setString(8, entity.awsAccessKeyID)
        insert.setString(9, entity.awsSecretAccessKey)
        insert.setLong(10, HashKeyUtils.connectionKey(entity))
        return insert
    }

    private fun map(res: ResultSet): ConnectionModel {
        val builder = ConnectionModel.builder()
            .id(res.getLong(1))
            .name(res.getString(2))
            .type(ConnectionType.valueOf(res.getString(3)))
            .path(res.getString(4))
            .jdbcUrl(res.getString(5))
            .jdbcDriver(res.getString(6))
            .jdbcUser(res.getString(7))
            .jdbcPass(res.getString(8))
            .awsAccessKeyID(res.getString(9))
            .awsSecretAccessKey(res.getString(10))

        val deactivatedTs = res.getTimestamp(11)
        if (deactivatedTs != null)
            builder.deactivatedTs(deactivatedTs.toLocalDateTime())

        return builder.build()
    }

    companion object {
        private val INSERT = """
            INSERT INTO CONNECTION(name,
                                   type,
                                   path,
                                   jdbc_url,
                                   jdbc_driver,
                                   jdbc_user,
                                   jdbc_pass,
                                   aws_access_key_id,
                                   aws_secret_access_key,
                                   hash_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT id,
                   name,
                   type,
                   path,
                   jdbc_url,
                   jdbc_driver,
                   jdbc_user,
                   jdbc_pass,
                   aws_access_key_id,
                   aws_secret_access_key,
                   deactivated_ts
            FROM CONNECTION
            WHERE id = ?
        """.trimIndent()

        private val SELECT_BY_HASH_KEY = """
            SELECT id,
                   name,
                   type,
                   path,
                   jdbc_url,
                   jdbc_driver,
                   jdbc_user,
                   jdbc_pass,
                   aws_access_key_id,
                   aws_secret_access_key,
                   deactivated_ts
            FROM CONNECTION
            WHERE deactivated_ts IS NULL 
                AND hash_key = ?
        """.trimIndent()


        private val SELECT_ALL_TABLE_DEPS = """
            SELECT C.id,
                   C.name,
                   C.type,
                   C.path,
                   C.jdbc_url,
                   C.jdbc_driver,
                   C.jdbc_user,
                   C.jdbc_pass,
                   C.aws_access_key_id,
                   C.aws_secret_access_key,
                   C.deactivated_ts
            FROM CONNECTION C
                     JOIN TARGET T on C.id = T.connection_id
                     JOIN DEPENDENCY D on T.id = D.dependency_target_id
            WHERE D.table_id = ?
        """.trimIndent()

        private const val DEACTIVATE_BY_ID =
            "UPDATE CONNECTION SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"


    }
}