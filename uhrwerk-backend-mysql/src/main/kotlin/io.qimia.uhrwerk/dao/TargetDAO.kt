package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.ConnectionService
import io.qimia.uhrwerk.common.metastore.config.TargetResult
import io.qimia.uhrwerk.common.metastore.config.TargetService
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.common.model.TargetModel
import io.qimia.uhrwerk.repo.TargetRepo
import java.sql.SQLException
import java.util.*

class TargetDAO() : TargetService {

    private val connService: ConnectionService = ConnectionDAO()

    private val repo: TargetRepo = TargetRepo()


    /**
     * Delete all targets for a given table. Assumes that targets can be removed (and perform no further checks)
     * @param tableId id for the table
     * @throws SQLException can throw database query errors
     */
    @Throws(SQLException::class)
    override fun deactivateByTableKey(tableKey: Long): Int? = repo.deactivateByTableKey(tableKey)


    /**
     * Save an array of targets for a particular table
     * @param targets all targets for some table
     * @param tableId id of the table
     * @param overwrite are overwrites allowed or not
     * @return result object showing what is stored, exceptions, if save was successful, and if not, why
     */
    override fun save(targets: List<TargetModel>,
                      tableKey: Long,
                      overwrite: Boolean): TargetResult {
        val saveResult = TargetResult()
        try {
            // enrich targets
            for (target in targets) {
                var newTargetConn: ConnectionModel? = null
                if (target.connection != null) {
                    newTargetConn =
                        connService.getByHashKey(HashKeyUtils.connectionKey(target.connection!!))
                }
                if (newTargetConn == null) {
                    saveResult.isSuccess = false
                    saveResult.isError = false
                    saveResult.message =
                        "Could not find connection name=${target.connection} for target-format " + target.format
                    return saveResult
                } else {
                    target.connection = newTargetConn
                    target.connectionKey = HashKeyUtils.connectionKey(target.connection!!)
                }
            }
            if (!overwrite) {
                val storedTargets = repo.getByTableKey(tableKey)
                // WARNING: Assumes Format is unique per table's targets
                if (storedTargets.isNotEmpty()) {
                    return compareStoredTargets(targets, storedTargets)
                }
                // else insert all Target values found
            }
            repo.save(targets)
            saveResult.isSuccess = true
            saveResult.isError = false
            saveResult.storedTargets = targets.toTypedArray()
        } catch (e: SQLException) {
            saveResult.isSuccess = false
            saveResult.isError = true
            saveResult.exception = e
            saveResult.message = e.message
        }
        return saveResult
    }

    /**
     * Retrieve a target by target-id
     *
     * @param id id (can be generated by Target)
     * @return Optional with Target if found and empty in all other cases
     */
    override fun get(id: Long): Optional<TargetModel> {
        var out = Optional.empty<TargetModel>()
        val target = repo.getById(id)
        if (target != null) {
            val conn = connService.getByHashKey(target!!.connectionKey!!)
            // WARNING: What if connection can't be found (should not be possible)
            target.connection = conn
            out = Optional.of(target)
        }
        return out
    }

    /**
     * Retrieve all targets set for a table
     * @param tableId id of table for which the targets are defined
     * @return array of Target model objects
     */
    override fun getByTableId(tableId: Long): List<TargetModel> {
        val targets = repo.getByTableId(tableId)
        if (!targets.isNullOrEmpty())
            targets.forEach {
                val conn = connService.getByHashKey(it.connectionKey!!)
                it.connection = conn
            }
        return targets
    }

    companion object {
        /**
         * Compare if two targets have the same format and same connection-name (which we assume to be unique)
         *
         * @return true if similar, false if not
         */
        @JvmStatic
        fun compareTargets(trueTarget: TargetModel, newTarget: TargetModel): Boolean {
            return trueTarget.format == newTarget.format && trueTarget.connection!!.name == newTarget.connection!!.name
        }

        /**
         * Compare given vs stored Targets. Assumes that both arrays are populated. For use when a save operation
         * becomes a check if anything has changed.
         *
         * @param givenTargets  targets handed to TargetDAO
         * @param storedTargets targets returned from the db
         * @return a TargetResult object like save should return
         */
        fun compareStoredTargets(
            givenTargets: List<TargetModel>,
            storedTargets: List<TargetModel>
        ): TargetResult {
            val result = TargetResult()

            val storedTargetLookup = storedTargets.associateBy { it.format }

            for (inputTarget in givenTargets) {
                val inputTargetFormat = inputTarget.format
                val matchingStoredTarget = storedTargetLookup[inputTargetFormat]

                if (matchingStoredTarget == null) {
                    result.isSuccess = false
                    result.isError = false
                    result.message =
                        "New target added which was not stored previously for format $inputTargetFormat"
                    return result
                }

                // NOTE: Only checks format and tableId (nothing connection based!)
                val compareRes = compareTargets(matchingStoredTarget, inputTarget)
                if (!compareRes) {
                    result.isSuccess = false
                    result.isError = false
                    result.message = "Target is not equal for format $inputTargetFormat"
                    return result
                }
            }
            result.isSuccess = true
            result.isError = false
            result.storedTargets = storedTargets.toTypedArray()
            return result
        }
    }
}