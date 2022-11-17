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
        secret.id = res.getLong(1)
        secret.name = res.getString(2)
        val secretType = res.getString(3)
        if (secretType != null && secretType.isNotEmpty())
            secret.type = SecretType.valueOf(secretType.uppercase())
        secret.awsSecretName = res.getString(4)
        secret.awsRegion = res.getString(5)
        secret.deactivatedTs = res.getTimestamp(6)?.toLocalDateTime()
        return secret
    }


    companion object {
        private const val INSERT =
            "INSERT INTO SECRET_ (name, type, aws_secret_name, aws_region,hash_key) VALUES(?,?,?,?,?)"

        private val SELECT_BY_ID = """
            SELECT id,
                   name,
                   type,
                   aws_secret_name,
                   aws_region,
                   deactivated_ts
            FROM SECRET_
            WHERE id = ?
        """.trimIndent()

        private val SELECT_BY_HASH_KEY = """
            SELECT id,
                   name,
                   type,
                   aws_secret_name,
                   aws_region,
                   deactivated_ts
            FROM SECRET_
            WHERE hash_key = ? AND deactivated_ts IS NULL
        """.trimIndent()

        private const val DEACTIVATE_BY_ID =
            "UPDATE SECRET_ SET deactivated_ts = CURRENT_TIMESTAMP() WHERE id = ?"


    }
}