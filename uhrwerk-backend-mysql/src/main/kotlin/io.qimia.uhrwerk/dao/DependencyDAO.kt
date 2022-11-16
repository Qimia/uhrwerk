package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.PartitionDurationTester
import io.qimia.uhrwerk.PartitionDurationTester.PartitionTestDependencyInput
import io.qimia.uhrwerk.PartitionDurationTester.PartitionTestResult
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
        tableId: Long,
        dependencies: Array<DependencyModel>
    ): ExistingDepRes {
        val existingRes = ExistingDepRes()
        val storedDeps = getByTableId(tableId)
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

    class FindTableRes {
        @JvmField
        var foundTables: List<TablePartRes>? = null

        @JvmField
        var missingNames: Set<String>? = null
    }

    /**
     * Find the tables for all the dependencies and check if the targets exists by querying on
     * target-id.
     *
     * @param dependencies dependencies that have to be found
     * @return FindQueryResult object with partition info about found tables and the names of the
     * missing tables
     * @throws SQLException can throw database query errors
     * @throws IllegalArgumentException
     */
    @Throws(SQLException::class, IllegalArgumentException::class)
    fun findTables(dependencies: Array<DependencyModel>): FindTableRes {
        val targetIds = dependencies.mapNotNull { it.dependencyTargetId }

        // TODO: Assumes tablenames unique in the context of a single table's dependencies
        // now assumes yes but in theory could be no -> then we need to map the missing target-ids back
        // to full table info
        val depTables = tableRepo.getAllByTargetIds(targetIds)
        val foundTables = depTables.map {
            val res = TablePartRes()
            res.tableId = it.id
            res.partitionUnit = it.partitionUnit
            res.partitionSize = it.partitionSize!!
            res.partitioned = it.partitioned
            res
        }

        val namesFound = depTables.mapNotNull { it.name }.toSet()
        var namesToFind = dependencies.mapNotNull { it.tableName }.toSet().subtract(namesFound)
        val results = FindTableRes()
        results.foundTables = foundTables
        results.missingNames = namesToFind
        return results
    }

    /**
     * Delete all dependencies for a given tableId
     *
     * @param tableId id of the table for which to delete the dependencies
     * @throws SQLException can throw database query errors
     */
    @Throws(SQLException::class)
    override fun deactivateByTableId(tableId: Long): Int? =
        dependencyRepo.deactivateByTableId(tableId)


    /**
     * Save all dependencies for a given table
     *
     * @param table the table
     * @param overwrite overwrite the previously stored dependencies or not
     * @return DependencyStoreResult object with stored objects, info about success, exceptions and
     * other results
     */
    override fun save(table: TableModel, overwrite: Boolean): DependencyStoreResult {
        // Assumes that new version means new tableId and thus any dependencies for the old version
        // won't be found
        val result = DependencyStoreResult()

        val depTableTargets = table.dependencies!!.flatMap { dep ->
            val tab = tableRepo.getByHashKey(HashKeyUtils.tableKey(dep))
            targetRepo.getByTableIdFormat(tab!!.id!!, dep.format!!)
                .map { tar -> Triple(dep, tab, tar) }
        }

        val dependencies = depTableTargets.map { (dep, tab, tar) ->
            dep.dependencyTable = tab
            dep.dependencyTableId = tab.id
            dep.dependencyTarget = tar
            dep.dependencyTargetId = tar.id
            dep
        }.toTypedArray()


        if (!overwrite) {
            // check if there are stored dependencies and if they are correct
            val checkDeps = checkExistingDependencies(table.id!!, dependencies)
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
            // if none are found continue adding the dependencies (and skip the remove-old-dependencies
            // step)
        }

        // overwrite or write needed -> first get all tables (abort if table not found)
        val tableSearchRes: FindTableRes
        try {
            tableSearchRes = findTables(dependencies)
        } catch (e: Exception) {
            result.isError = true
            result.exception = e
            result.message = e.message
            return result
        }
        if (tableSearchRes.missingNames!!.isNotEmpty()) {
            result.isSuccess = false
            result.message =
                "Missing tables: " + tableSearchRes.missingNames.toString() + " (based on id)"
            return result
        }
        if (table.partitioned) {
            // Check dependency sizes (abort if size does not match)
            val sizeTestResult = checkPartitionSizes(
                dependencies,
                table.partitionUnit,
                table.partitionSize!!,
                tableSearchRes.foundTables
            )
            if (!sizeTestResult.success) {
                result.isSuccess = false
                result.isError = false
                result.message = String.format(
                    "Tables %s have the wrong partition duration",
                    sizeTestResult.badTableNames.joinToString()
                )
                return result
            }
        } else {
            // Check if there are any dependencies which have a transformation (and/or are themselves
            // partitioned)
            var problem = false
            val problemDependencies = ArrayList<String>()
            val tablePartitionLookup = tableSearchRes.foundTables!!.associateBy { it.tableId }
            for (checkDep in dependencies) {
                val connectedTable = tablePartitionLookup[checkDep.dependencyTableId]
                if (checkDep.transformType != PartitionTransformType.NONE
                    || connectedTable!!.partitioned
                ) {
                    problem = true
                    problemDependencies.add(checkDep.tableName!!)
                }
            }
            if (problem) {
                result.isSuccess = false
                result.isError = false
                result.message = "Tables " + java.lang.String.join(
                    ", ",
                    problemDependencies
                ) + " have a partitioning problem"
                return result
            }
        }
        // If it is a table without partitioning, the fact that they exist should be enough

        // Delete all old dependencies (in case of overwrite) and insert new list
        try {
            if (overwrite) {
                deactivateByTableId(table.id!!)
            }
            result.dependenciesSaved = dependencyRepo.save(dependencies.toList())?.toTypedArray()
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
            if (trueDep.transformType != newDep.transformType) {
                result.success = false
                problemString.append("dependency: ").append(newDepName)
                    .append("\thas a different type\n")
            }
            if (trueDep.transformPartitionSize != newDep.transformPartitionSize) {
                result.success = false
                problemString.append("dependency: ").append(newDepName)
                    .append("\thas a different size\n")
            }
            // All other ID's in Dependency are set based on these parameters (and should be the same if
            // these are the same)
            if (!result.success) {
                result.problem = problemString.toString()
            }
            return result
        }

        /**
         * Test the dependencies partition sizes by giving the partition size of the target table and the
         * information of the dependencies' tables and the dependencies'(transformations)
         *
         * @param dependencies dependencies that need to be tested
         * @param partitionUnit the partition unit of the output table
         * @param partitionSize the partition size (how many times unit) of the output table
         * @param dependencyTables info about the partition size of each of the dependencies' tables
         * @return PartitionTestResult showing if they were all good or which tables (-names) did not
         * match up
         */
        @JvmStatic
        fun checkPartitionSizes(
            dependencies: Array<DependencyModel>,
            partitionUnit: PartitionUnit?,
            partitionSize: Int,
            dependencyTables: List<TablePartRes>?
        ): PartitionTestResult {
            val depTableIdLookup = dependencies.associateBy { it.dependencyTableId }

            val partitionTestInputs = dependencyTables!!.map { tableRes ->
                val newTest = PartitionTestDependencyInput()
                val dependency = depTableIdLookup[tableRes.tableId]

                newTest.dependencyTableName = dependency!!.tableName
                newTest.transformType = dependency.transformType
                newTest.transformSize = dependency.transformPartitionSize!!

                // TODO: Doesn't support transform-partition-unit (and transforms using that) for now
                newTest.dependencyTablePartitionSize = tableRes.partitionSize
                newTest.dependencyTablePartitionUnit = tableRes.partitionUnit
                newTest.dependencyTablePartitioned = tableRes.partitioned
                newTest
            }

            return PartitionDurationTester.checkDependencies(
                partitionUnit,
                partitionSize,
                partitionTestInputs
            )
        }
    }
}