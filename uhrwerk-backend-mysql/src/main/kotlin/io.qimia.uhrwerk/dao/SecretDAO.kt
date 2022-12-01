package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.SecretResult
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.metastore.model.SecretModel
import io.qimia.uhrwerk.repo.SecretRepo
import java.sql.SQLException

class SecretDAO {
    private val repo = SecretRepo()

    fun getByHashKey(hashKey: Long): SecretModel? =
        repo.getByHashKey(hashKey)

    fun getByName(name:String): SecretModel? =
        repo.getByHashKey(HashKeyUtils.secretKey(name))

    fun getById(id: Long): SecretModel? {
        return repo.getById(id)
    }


    fun save(secret: SecretModel, overwrite: Boolean): SecretResult {
        val result = SecretResult()
        result.oldSecret = getByHashKey(HashKeyUtils.secretKey(secret))
        try {
            if (!overwrite && result.oldSecret != null) {
                result.message = String.format(
                    "An Active Secret with name=%s already exists in the Metastore.",
                    secret.name
                )
                return result
            }
            if (result.oldSecret != null)
                repo.deactivateById(result.oldSecret.id!!)
            result.newSecret = repo.save(secret)
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