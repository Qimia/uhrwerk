package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.SecretModel
import io.qimia.uhrwerk.common.metastore.model.SecretType
import java.sql.PreparedStatement
import java.sql.ResultSet

class SecretRepo : BaseRepo<SecretModel>() {
    fun save(secret: SecretModel): SecretModel? =
        super.insert(secret, INSERT) {
            insertParams(secret, it)
        }

    fun save(secrets: List<SecretModel>): List<SecretModel> =
        secrets.map { save(it)!! }

    fun getById(id: Long): SecretModel? =
        super.find(
            SELECT_BY_ID, {
                it.setLong(1, id)
            }, this::map
        )

    fun getByHashKey(hashKey: Long): SecretModel? =
        super.getByHashKey(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun deactivateById(id: Long): Int? =
        super.update(
            DEACTIVATE_BY_ID,
        ) {
            it.setLong(1, id)
        }


    private fun insertParams(
        entity: SecretModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setString(1, entity.name)
        insert.setString(2, entity.type!!.name)
        insert.setString(3, entity.awsSecretName)
        insert.setString(4, entity.awsRegion)
        insert.setLong(5, HashKeyUtils.secretKey(entity))
        return insert
    }

    private fun map(res: ResultSet): SecretModel {
        val secret = SecretModel()
        secret.id = res.getLong("id")
        secret.name = res.getString("name")
        val secretType = res.getString("type")
        if (secretType != null && secretType.isNotEmpty())
            secret.type = SecretType.valueOf(secretType.uppercase())
        secret.awsSecretName = res.getString("aws_secret_name")
        secret.awsRegion = res.getString("aws_region")
        secret.deactivatedTs = res.getTimestamp("deactivated_ts")?.toLocalDateTime()
        return secret
    }


    companion object {
        private val COLUMNS = listOf(
            "id",
            "name",
            "type",
            "aws_secret_name",
            "aws_region",
            "hash_key",
            "deactivated_ts"
        )

        private val COLUMNS_STR = columnsToString(COLUMNS)

        private val INSERT = insertSql("SECRET_", COLUMNS)


        private val SELECT_BY_ID = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SECRET_ \n" +
                "WHERE id = ?"

        private val SELECT_BY_HASH_KEY = "SELECT \n" +
                "$COLUMNS_STR \n" +
                "FROM SECRET_ \n" +
                "WHERE hash_key = ? AND deactivated_ts IS NULL"

        private const val DEACTIVATE_BY_ID =
            "UPDATE SECRET_ SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"


    }
}