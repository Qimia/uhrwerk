package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.*
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult
import io.qimia.uhrwerk.common.metastore.dependency.TableDependencyService
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResult
import io.qimia.uhrwerk.common.metastore.dependency.TablePartitionResultSet
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.repo.TablePartition
import io.qimia.uhrwerk.repo.TablePartitionRepo
import io.qimia.uhrwerk.repo.TableRepo
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.sql.*
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors

class TableDAO : TableDependencyService, TableService {
    private val partService: PartitionService = PartitionDAO()
    private val connService: ConnectionService = ConnectionDAO()
    private val targetService: TargetService = TargetDAO()
    private val dependencyDAO: DependencyService = DependencyDAO()
    private val sourceService: SourceService = SourceDAO()
    private val tableRepo = TableRepo()
    private val tablePartRepo = TablePartitionRepo()
    private val logger: Logger

    override fun save(table: TableModel, overwrite: Boolean): TableResult {
        val tableResult = TableResult()
        tableResult.isSuccess = true
        tableResult.isError = false
        tableResult.newResult = table
        val tableId = table.id
        try {
            if (!overwrite) {
                saveTablesArrays(table, tableResult, overwrite)
                val oldTable = tableRepo.getByHashKey(HashKeyUtils.tableKey(table)).first()
                if (oldTable != null) {
                    tableResult.oldResult = oldTable
                    if (oldTable != table) {
                        val message = String.format(
                            """
                                     A Table with id=%d and different values already exists in the Metastore.
                                     
                                     Passed Table:
                                     %s
                                     
                                     Table in the Metastore:
                                     %s
                                     """.trimIndent(),
                            tableId,
                            table.toString(),
                            oldTable.toString()
                        ) // todo improve finding differences
                        tableResult.message = message
                        tableResult.isSuccess = false
                    }
                    return tableResult
                }
                tableResult.newResult = tableRepo.save(table)
            } else {
                tableRepo.deactivateById(tableId)
                saveTablesArrays(table, tableResult, overwrite)
                tableResult.newResult = tableRepo.save(table)
            }
        } catch (e: SQLException) {
            tableResult.isError = true
            tableResult.isSuccess = false
            tableResult.exception = e
            tableResult.message = e.message
        } catch (e: NullPointerException) {
            tableResult.isError = true
            tableResult.isSuccess = false
            tableResult.exception = e
            tableResult.message = e.message
        }
        return tableResult
    }

    private fun saveTablesArrays(table: TableModel, tableResult: TableResult, overwrite: Boolean) {
        if (table.targets != null && table.targets.isNotEmpty()) {
            val targetResult = targetService.save(table.targets.toList(), table.id, overwrite)
            tableResult.targetResult = targetResult
            if (targetResult.isSuccess) {
                table.targets = targetResult.storedTargets
            } else {
                tableResult.isSuccess = false
                return
            }
        }
        if (table.dependencies != null && table.dependencies.isNotEmpty()) {
            val dependencyResult = dependencyDAO.save(table, overwrite)
            tableResult.dependencyResult = dependencyResult
            if (dependencyResult.isSuccess) {
                table.dependencies = dependencyResult.dependenciesSaved
            } else {
                tableResult.isSuccess = false
                return
            }
        }
        if (table.sources != null && table.sources.isNotEmpty()) {
            val sourceResults = sourceService.save(table.sources.toList(), table, overwrite)
            tableResult.sourceResults = sourceResults.toTypedArray()
            for (i in sourceResults.indices) {
                if (sourceResults[i].isSuccess) {
                    table.sources[i] = sourceResults[i].newResult
                } else {
                    tableResult.isSuccess = false
                    return
                }
            }
        }
    }

    /**
     * Adapted version of [processingPartitions][.processingPartitions] When a table does not
     * have a partitioning-scheme itself, this checks if itself has been processed and if not, if the
     * dependencies have been processed (Current version checks if it has been processed at any time)
     *
     * @param table table which needs to be processed
     * @return same TablePartitionResultSet but with a single reference to failed or succeeded result
     */
    @Throws(SQLException::class)
    private fun processUnpartitionedTable(
        table: TableModel,
        requestTime: LocalDateTime
    ): TablePartitionResultSet {
        assert(!table.isPartitioned) { "Table can't be partitioned for partitionless processing" }
        val singleResult = TablePartitionResult()
        val resultSet = TablePartitionResultSet()
        // FIXME checks only the first target of the table (see FIXME normal processingPartitions)
        val processedPartition = partService.getLatestPartition(table.targets[0].id)
        if (processedPartition != null) {
            // If partition found -> check if it has been processed in the last 1 minute
            val requestDiff = Duration.between(processedPartition.partitionTs, requestTime)
            if (requestDiff.toSeconds() < 60L) {
                // Warning !! (Does **not** add dependency and resolution info)
                singleResult.isProcessed = true
                singleResult.partitionTs = processedPartition.partitionTs
                resultSet.processed = arrayOf(singleResult)
                resultSet.processedTs = arrayOf(processedPartition.partitionTs)
                return resultSet
            }
            // If not last-1-minute than continue as if none were found
        }
        singleResult.isProcessed = false

        // If there are no dependencies then this table is resolved and ready to run
        if (table.dependencies == null || table.dependencies.isEmpty()) {
            singleResult.isResolved = true
            singleResult.partitionTs = requestTime
            singleResult.resolvedDependencies = arrayOfNulls(0)
            singleResult.failedDependencies = arrayOfNulls(0)
            resultSet.resolved = arrayOf(singleResult)
            resultSet.resolvedTs = arrayOf(requestTime)
            return resultSet
        }
        val tablePartitionSpecs = tablePartRepo.getTablePartitions(table.id)
        val connections = connService.getAllTableDeps(table.id)

        val connectionsMap = connections!!.map { it.id to it }.toMap()

        val dependenciesMap = Arrays.stream(table.dependencies).collect(
            Collectors.toMap(Function { obj: DependencyModel -> obj.id },
                Function { x: DependencyModel? -> x })
        )

        // Go over each dependency (+ spec) and check if it's there or not
        val resolvedDependencies = ArrayList<DependencyResult>()
        val failedDependencies = ArrayList<DependencyResult>()
        var singleSuccess = true
        for (j in tablePartitionSpecs.indices) {
            val spec = tablePartitionSpecs[j]
            assert(spec.transformType == PartitionTransformType.NONE) { "Can't have partitioned dependencies for tables without partitioning" }
            val dependencyResult = DependencyResult()
            dependencyResult.connection = connectionsMap[spec.connectionId]
            dependencyResult.dependency = dependenciesMap[spec.dependencyId]
            val depPartition = partService.getLatestPartition(spec.targetId)
            if (depPartition == null) {
                dependencyResult.isSuccess = false
                dependencyResult.failed = arrayOf(requestTime)
                failedDependencies.add(dependencyResult)
                singleSuccess = false
            } else {
                dependencyResult.isSuccess = true
                dependencyResult.succeeded = arrayOf(depPartition.partitionTs)
                dependencyResult.partitionTs = depPartition.partitionTs
                dependencyResult.partitions = arrayOf(depPartition)
                resolvedDependencies.add(dependencyResult)
            }
        }
        singleResult.resolvedDependencies = resolvedDependencies.toTypedArray()
        singleResult.failedDependencies = failedDependencies.toTypedArray()
        singleResult.isResolved = singleSuccess
        singleResult.partitionTs = requestTime
        if (singleSuccess) {
            resultSet.resolved = arrayOf(singleResult)
            resultSet.resolvedTs = arrayOf(requestTime)
        } else {
            resultSet.failed = arrayOf(singleResult)
            resultSet.failedTs = arrayOf(requestTime)
        }
        return resultSet
    }

    /**
     * Adapted version of [processingPartitions][.processingPartitions] When a table has no
     * dependencies, all we need to do is check if current table has already been processed for each
     * requested partitionTS
     *
     * @param table table which needs to be processed
     * @param requestedPartitionTs list of partition starting timestamps
     * @return all info required for running a table
     * @throws SQLException
     */
    private fun processNoDependencyPartitions(
        table: TableModel, requestedPartitionTs: List<LocalDateTime>
    ): TablePartitionResultSet {
        val processedPartitions =
            partService.getPartitions(table.targets[0].id, requestedPartitionTs)
        val processedTs = TreeSet<LocalDateTime>()
        for (processedPartition in processedPartitions) {
            processedTs.add(processedPartition.partitionTs)
        }
        val resolvedTs: MutableList<LocalDateTime> = ArrayList()
        val resolved: MutableList<TablePartitionResult> = ArrayList()
        val processed: MutableList<TablePartitionResult> = ArrayList()
        for (requestedPartitionT in requestedPartitionTs) {
            val tablePartitionResult = TablePartitionResult()
            tablePartitionResult.partitionTs = requestedPartitionT
            tablePartitionResult.isResolved = true
            tablePartitionResult.failedDependencies = arrayOfNulls(0)
            tablePartitionResult.resolvedDependencies = arrayOfNulls(0)
            if (processedTs.contains(requestedPartitionT)) {
                tablePartitionResult.isProcessed = true
                processed.add(tablePartitionResult)
            } else {
                tablePartitionResult.isProcessed = false
                resolved.add(tablePartitionResult)
                resolvedTs.add(tablePartitionResult.partitionTs)
            }
        }
        val tablePartitionResultSet = TablePartitionResultSet()
        tablePartitionResultSet.processed = processed.toTypedArray()
        tablePartitionResultSet.resolved = resolved.toTypedArray()
        tablePartitionResultSet.failed = arrayOfNulls(0)
        tablePartitionResultSet.processedTs = processedTs.toTypedArray()
        tablePartitionResultSet.resolvedTs = resolvedTs.toTypedArray()
        tablePartitionResultSet.failedTs = arrayOfNulls(0)
        return tablePartitionResultSet
    }

    /**
     * Find out which partitions have already been processed for the current table, which are ready to
     * be processed and which ones can't be processed yet. This check is being done on a list of given
     * partitions which the caller want to check.
     *
     * @param table a Table which needs to be processed / produced
     * @param requestedPartitionTs which partitions need to be processed for this table
     * @return TablePartitionResultSet containing all info required for the processing and failure
     * reporting
     * @throws SQLException
     */
    @Throws(SQLException::class)
    override fun processingPartitions(
        table: TableModel, requestedPartitionTs: List<LocalDateTime>
    ): TablePartitionResultSet {
        if (!table.isPartitioned) {
            return processUnpartitionedTable(table, requestedPartitionTs[0])
        }
        if (table.dependencies == null || table.dependencies.size == 0) {
            return processNoDependencyPartitions(table, requestedPartitionTs)
        }

        // Check what partitions have already been processed
        // FIXME which target for the table should be used for getting (already) processed partition of
        // the table
        val processedPartitions =
            partService.getPartitions(table.targets[0].id, requestedPartitionTs)
        val processedTs = TreeSet<LocalDateTime?>()
        for (i in processedPartitions.indices) {
            processedTs.add(processedPartitions[i].partitionTs)
        }

        // Get full spec-objects for each of the dependencies + store all connections
        val tablePartitionSpecs = tablePartRepo.getTablePartitions(table.id)
        val connections = connService.getAllTableDeps(table.id)

        val connectionsMap = connections!!.map { it.id to it }.toMap()

        val tableDependencies = table.dependencies

        if (tablePartitionSpecs.size != tableDependencies.size) {
            logger.error("Could not find all specifications for all dependencies in metastore")
            return buildResultSet(listOf(), listOf(), setOf())
        }
        val dependenciesMap: MutableMap<Long, DependencyModel> = HashMap()
        for (i in tableDependencies.indices) {
            dependenciesMap[tableDependencies[i].id] = tableDependencies[i]
        }
        val dependencyResults = mutableListOf<List<DependencyResult>>()
        for (spec: TablePartition in tablePartitionSpecs) {
            val tmpRes = mutableListOf<DependencyResult>()
            if (spec.transformType == PartitionTransformType.NONE) {
                // If not partitioned then check single
                val depPartition = partService.getLatestPartition(spec.targetId)
                if (depPartition == null) {
                    // If there is nothing, set all to unsuccessful
                    val dependencyResult = DependencyResult()
                    dependencyResult.connection = connectionsMap[spec.connectionId]
                    dependencyResult.dependency = dependenciesMap[spec.dependencyId]
                    dependencyResult.isSuccess = false
                    dependencyResult.failed = arrayOf()
                    for (i in requestedPartitionTs.indices) {
                        tmpRes.add(dependencyResult)
                    }
                } else {
                    // If there is something, check for every requested partition ->
                    // Is the found dependency-partition's startTS equal or after the endpoint of that partition
                    val depPartitionSnapshotTime = depPartition.partitionTs
                    val tableChronoUnit = ChronoUnit.valueOf(table.partitionUnit.name)
                    val tableDuration = Duration.of(table.partitionSize.toLong(), tableChronoUnit)
                    for (i in requestedPartitionTs.indices) {
                        val dependencyResult = DependencyResult()
                        dependencyResult.connection = connectionsMap[spec.connectionId]
                        dependencyResult.dependency = dependenciesMap[spec.dependencyId]
                        val requestedPartitionEndTs = requestedPartitionTs[i].plus(tableDuration)
                        if (depPartitionSnapshotTime.isEqual(requestedPartitionEndTs) || depPartitionSnapshotTime.isAfter(
                                requestedPartitionEndTs
                            )
                        ) {
                            dependencyResult.isSuccess = true
                            dependencyResult.succeeded = arrayOf(depPartition.partitionTs)
                            dependencyResult.partitionTs = depPartitionSnapshotTime
                            dependencyResult.partitions = arrayOf(depPartition)
                        } else {
                            dependencyResult.isSuccess = false
                            dependencyResult.failed = arrayOf(depPartitionSnapshotTime)
                            dependencyResult.partitionTs = depPartitionSnapshotTime
                            dependencyResult.partitions = arrayOf(depPartition)
                        }
                        tmpRes.add(dependencyResult)
                    }
                }
            } else {
                // partitioned so first create list of which partitions to check
                val partitionTs = JdbcBackendUtils.dependencyPartitions(
                    requestedPartitionTs,
                    table.partitionUnit,
                    table.partitionSize,
                    spec.partitionUnit,
                    spec.partitionSize,
                    spec.transformType,
                    spec.transformSize
                )

                for (partTs: List<LocalDateTime> in partitionTs) {
                    // each partition spec check every partition
                    val depPartitions = partService.getPartitions(spec.targetId, partTs)
                    val dependencyResult = DependencyResult()
                    dependencyResult.connection = connectionsMap[spec.connectionId]
                    dependencyResult.dependency = dependenciesMap[spec.dependencyId]
                    if (depPartitions.size == spec.transformSize) {
                        dependencyResult.isSuccess = true
                        dependencyResult.succeeded = partTs.toTypedArray()
                        dependencyResult.partitions = depPartitions.toTypedArray()
                    } else {
                        val succeeded = mutableListOf<LocalDateTime>()
                        if (depPartitions.size > 0) {
                            dependencyResult.partitions = depPartitions.toTypedArray()
                            for (partition in depPartitions) {
                                val ts = partition.partitionTs
                                succeeded.add(ts)
                            }
                        }
                        val failed = partTs.subtract(succeeded)
                        dependencyResult.isSuccess = false
                        dependencyResult.failed = failed.toTypedArray()
                        dependencyResult.succeeded = succeeded.toTypedArray()
                    }
                    tmpRes.add(dependencyResult)
                }
            }
            dependencyResults.add(tmpRes)
        }
        return buildResultSet(requestedPartitionTs, dependencyResults, processedTs)
    }

    init {
        logger = LoggerFactory.getLogger(this.javaClass)
    }

    companion object {
        /**
         * Build a TablePartitionResultSet out of DependencyResults and processed timestamps
         *
         * @param requestedPartitionTs Originally requested partition timestamps for processing
         * @param dependencyResults DependencyResults size [# requested partitions][# of dependencies]
         * @param processedTs Which timestamps have already been
         * @return
         */
        fun buildResultSet(
            requestedPartitionTs: List<LocalDateTime>,
            dependencyResults: List<List<DependencyResult>>,
            processedTs: Set<LocalDateTime?>
        ): TablePartitionResultSet {

            val resolvedTs: MutableList<LocalDateTime> = ArrayList()
            val failedTs: MutableList<LocalDateTime> = ArrayList()
            val resolved: MutableList<TablePartitionResult> = ArrayList()
            val processed: MutableList<TablePartitionResult> = ArrayList()
            val failed: MutableList<TablePartitionResult> = ArrayList()

            for (i in requestedPartitionTs.indices) {
                val tablePartitionResult = TablePartitionResult()
                val partitionTs = requestedPartitionTs[i]
                tablePartitionResult.partitionTs = partitionTs
                val results = dependencyResults[i]
                var success = true
                val resolvedDependencies: MutableList<DependencyResult?> = ArrayList()
                val failedDependencies: MutableList<DependencyResult?> = ArrayList()
                for (result in results) {
                    success = success and result.isSuccess
                    if (result.isSuccess) resolvedDependencies.add(result) else failedDependencies.add(
                        result
                    )
                }
                if (success) {
                    tablePartitionResult.isResolved = true
                    tablePartitionResult.resolvedDependencies = results.toTypedArray()
                } else {
                    tablePartitionResult.resolvedDependencies = resolvedDependencies.toTypedArray()
                    tablePartitionResult.failedDependencies = failedDependencies.toTypedArray()
                }
                if (processedTs.contains(partitionTs)) {
                    tablePartitionResult.isProcessed = true
                    processed.add(tablePartitionResult)
                } else if (tablePartitionResult.isResolved) {
                    resolved.add(tablePartitionResult)
                    resolvedTs.add(tablePartitionResult.partitionTs)
                } else {
                    failed.add(tablePartitionResult)
                    failedTs.add(tablePartitionResult.partitionTs)
                }
            }
            val tablePartitionResultSet = TablePartitionResultSet()
            tablePartitionResultSet.processed = processed.toTypedArray()
            tablePartitionResultSet.resolved = resolved.toTypedArray()
            tablePartitionResultSet.failed = failed.toTypedArray()
            tablePartitionResultSet.processedTs = processedTs.toTypedArray()
            tablePartitionResultSet.resolvedTs = resolvedTs.toTypedArray()
            tablePartitionResultSet.failedTs = failedTs.toTypedArray()
            return tablePartitionResultSet
        }

    }
}