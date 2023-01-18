package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.ConnectionType
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import java.sql.PreparedStatement
import java.sql.ResultSet

class ConnectionRepo : BaseRepo<ConnectionModel>() {
    fun save(connection: ConnectionModel): ConnectionModel? =
        super.insert(connection, INSERT) {
            insertParams(connection, it)
        }

    fun getByHashKey(hashKey: Long): ConnectionModel? =
        super.getByHashKey(
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
        insert.setString(2, entity.type!!.name)
        insert.setString(3, entity.path)
        // jdbc columns values
        insert.setString(4, entity.jdbcUrl)
        insert.setString(5, entity.jdbcDriver)
        insert.setString(6, entity.jdbcUser)
        insert.setString(7, entity.jdbcPass)
        // aws columns values
        insert.setString(8, entity.awsAccessKeyID)
        insert.setString(9, entity.awsSecretAccessKey)
        insert.setString(10, entity.redshiftFormat)
        insert.setString(11, entity.redshiftAwsIamRole)
        insert.setString(12, entity.redshiftTempDir)
        insert.setLong(13, HashKeyUtils.connectionKey(entity))
        return insert
    }

    private fun map(res: ResultSet): ConnectionModel {
        val conn = ConnectionModel()
        conn.id = res.getLong(1)
        conn.name = res.getString(2)
        conn.type = ConnectionType.valueOf(res.getString(3))
        conn.path = res.getString(4)
        conn.jdbcUrl = res.getString(5)
        conn.jdbcDriver = res.getString(6)
        conn.jdbcUser = res.getString(7)
        conn.jdbcPass = res.getString(8)
        conn.awsAccessKeyID = res.getString(9)
        conn.awsSecretAccessKey = res.getString(10)
        conn.redshiftFormat = res.getString(11)
        conn.redshiftAwsIamRole = res.getString(12)
        conn.redshiftTempDir = res.getString(13)
        conn.deactivatedTs = res.getTimestamp(14)?.toLocalDateTime()
        return conn
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
                                   redshift_format,
                                   redshift_aws_iam_role,
                                   redshift_temp_dir,
                                   hash_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                   redshift_format,
                   redshift_aws_iam_role,
                   redshift_temp_dir,
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
                   redshift_format,
                   redshift_aws_iam_role,
                   redshift_temp_dir,
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
                   C.redshift_format,
                   C.redshift_aws_iam_role,
                   C.redshift_temp_dir,
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