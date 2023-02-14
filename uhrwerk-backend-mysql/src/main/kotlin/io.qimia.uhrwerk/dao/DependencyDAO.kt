package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.DependencyService
import io.qimia.uhrwerk.common.metastore.config.DependencyStoreResult
import io.qimia.uhrwerk.common.metastore.model.*
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.repo.DependencyRepo
import io.qimia.uhrwerk.repo.TableRepo
import io.qimia.uhrwerk.repo.TargetRepo
import java.sql.SQLException
import java.util.*

class DependencyDAO() : DependencyService {

    private val tableRepo: TableRepo = TableRepo()
    private val targetRepo: TargetRepo = TargetRepo()
    private val dependencyRepo: DependencyRepo = DependencyRepo()

    class DepCompareRes {
        @JvmField
        var success = false

        @JvmField
        var problem: String? = null
    }

    class ExistingDepRes {
        @JvmField
        var found = false

        @JvmField
        var correct = false
        var problems: String? = null
    }

    /**
     * Check if there are already dependencies stored for a table and see if they match the
     * dependencies given for storage to DependencyDAO
     *
     * @param tableId id of table for which the dependencies are defined
     * @param dependencies dependencies given that need to be checked
     * @return The result of this check showing if the dependencies were found, if the found ones are
     * correct. If they are not correct, shows what exactly is wrong (can be multi line String!)
     */
    fun checkExistingDependencies(
        tableKey: Long,
        dependencies: Array<DependencyModel>
    ): ExistingDepRes {
        val existingRes = ExistingDepRes()
        val storedDeps = getByTableKey(tableKey)
        if (storedDeps.isEmpty()) {
            existingRes.found = false
            existingRes.correct = true
            return existingRes
        }

        // If found, match with given dependencies and check if they are the same
        existingRes.found = true
        existingRes.correct = true
        existingRes.problems = ""

        // TODO: Assumes tablenames unique in the context of a single table's dependencies
        // see findTables comment
        val storedDepLookup = HashMap<String, DependencyModel>()
        for (storedDep in storedDeps) {
            storedDepLookup[storedDep.tableName!!] = storedDep
        }
        val problemString = StringBuilder()
        for (inDep in dependencies) {
            val depTableName = inDep.tableName
            if (!storedDepLookup.containsKey(depTableName)) {
                existingRes.correct = false
                problemString.append("dependency:").append(depTableName)
                    .append("\twas later added\n")
                continue
            }
            val foundDep = storedDepLookup[depTableName]
            val compareRes = compareDependency(foundDep, inDep)
            if (!compareRes.success) {
                existingRes.correct = false
                problemString.append(compareRes.problem)
            }
        }
        if (!existingRes.correct) {
            existingRes.problems = problemString.toString()
        }
        return existingRes
    }

    class TablePartRes {
        @JvmField
        var tableId: Long? = null
        var partitioned = false

        @JvmField
        var partitionUnit: PartitionUnit? = null

        @JvmField
        var partitionSize = 0
    }



    /**
     * Delete all dependencies for a given tableId
     *
     * @param tableId id of the table for which to delete the dependencies
     * @throws SQLException can throw database query errors
     */
    @Throws(SQLException::class)
    override fun deactivateByTableKey(tableKey: Long): Int? =
        dependencyRepo.deactivateByTableKey(tableKey)


    /**
     * Save all dependencies for a given table
     *
     * @param table the table
     * @param overwrite overwrite the previously stored dependencies or not
     * @return DependencyStoreResult object with stored objects, info about success, exceptions and
     * other results
     */
    override fun save(
        tableId: Long,
        tableKey: Long,
        dependencies: Array<DependencyModel>?,
        overwrite: Boolean
    ): DependencyStoreResult {
        // Assumes that new version means new tableId and thus any dependencies for the old version
        // won't be found
        val result = DependencyStoreResult()

        val enrichedDependencies = dependencies!!.flatMap { dependency ->
            val depTableKey = HashKeyUtils.tableKey(dependency)
            dependency.dependencyTableKey = depTableKey
            val depTable = tableRepo.getByHashKey(depTableKey)
            targetRepo.getByTableKeyFormat(depTableKey, dependency.format!!)
                .map { depTarget ->
                    val depTargetKey = HashKeyUtils.targetKey(depTarget)
                    dependency.dependencyTargetKey = depTargetKey
                    dependency
                }
        }



        if (!overwrite) {
            // check if there are stored dependencies and if they are correct
            val checkDeps = checkExistingDependencies(tableKey, dependencies)
            if (checkDeps.found && !checkDeps.correct) {
                result.isSuccess = false
                result.isError = false
                result.message = checkDeps.problems
                return result
            } else if (checkDeps.found && checkDeps.correct) {
                result.isSuccess = true
                result.isError = false
                result.dependenciesSaved = dependencies
                return result
            }
        }

        try {
            result.dependenciesSaved =
                dependencyRepo.save(enrichedDependencies.toList())?.toTypedArray()
            result.isSuccess = true
            result.isError = false
        } catch (e: Exception) {
            result.isSuccess = false
            result.isError = true
            result.exception = e
            result.message = e.message
        }
        return result
    }

    /**
     * Retrieve all stored dependencies for a given table
     *
     * @param tableId tableId of the table for which the dependencies are returned
     * @return model Dependency objects
     */
    override fun getByTableId(tableId: Long): List<DependencyModel> =
        dependencyRepo.getByTableId(tableId)

    override fun getByTableKey(tableKey: Long): List<DependencyModel> =
        dependencyRepo.getByTableKey(tableKey)

    companion object {

        /**
         * Compare two dependencies and report any discrepancies
         *
         * @param trueDep the gold standard dependency
         * @param newDep the other dependency which is compared
         * @return Result denoting success or not, and if not what is exactly different
         */
        @JvmStatic
        fun compareDependency(trueDep: DependencyModel?, newDep: DependencyModel): DepCompareRes {
            val result = DepCompareRes()
            result.success = true
            val problemString = StringBuilder()
            val newDepName = newDep.tableName
            if (trueDep!!.tableId != newDep.tableId) {
                result.success = false
                problemString
                    .append("dependency: ")
                    .append(newDepName)
                    .append("\thas a different table-id\n")
            }
            if (trueDep.area != newDep.area) {
                result.success = false
                problemString.append("dependency: ").append(newDepName)
                    .append("\thas a different area\n")
            }
            if (trueDep.vertical != newDep.vertical) {
                result.success = false
                problemString
                    .append("dependency: ")
                    .append(newDepName)
                    .append("\thas a different vertical\n")
            }
            if (trueDep.tableName != newDep.tableName) {
                result.success = false
                problemString
                    .append("dependency: ")
                    .append(newDepName)
                    .append("\thas a different table name\n")
            }
            if (trueDep.format != newDep.format) {
                result.success = false
                problemString.append("dependency: ").append(newDepName)
                    .append("\thas a different format\n")
            }
            if (trueDep.version != newDep.version) {
                result.success = false
                problemString.append("dependency: ").append(newDepName)
                    .append("\thas a different version\n")
            }
            // All other ID's in Dependency are set based on these parameters (and should be the same if
            // these are the same)
            if (!result.success) {
                result.problem = problemString.toString()
            }
            return result
        }


    }
}