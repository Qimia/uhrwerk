package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.builders.PartitionDependencyBuilder
import io.qimia.uhrwerk.common.metastore.config.PartitionDependencyResult
import io.qimia.uhrwerk.common.metastore.config.PartitionDependencyService
import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult
import io.qimia.uhrwerk.repo.PartitionDependencyRepo
import java.sql.SQLException

class PartitionDependencyDAO : PartitionDependencyService {
    val repo = PartitionDependencyRepo()

    @Throws(SQLException::class)
    private fun storeAll(childPartitionId: Long, dependencies: Array<DependencyResult>) {
        val partitionDependencies = dependencies.toList().flatMap { dependency ->
            dependency.partitions.map {
                PartitionDependencyBuilder()
                    .partitionId(childPartitionId)
                    .dependencyPartitionId(it.id)
                    .build()
            }
        }
        repo.save(partitionDependencies)
    }

    /**
     * Remove all partition-dependencies tied to a single child partition
     *
     * @param childPartitionId id of the child partition
     * @throws SQLException
     */
    @Throws(SQLException::class)
    private fun deleteByChildId(childPartitionId: Long) = repo.deleteByChildId(childPartitionId)


    /**
     * Create partition dependencies for all the dependencies of a given child partition
     * Warning: Does not store the Partitions themselves
     *
     * @param childPartitionId id of the child partition object
     * @param dependencies     Array of DependencyResult for each of the dependencies
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    override fun saveAll(
        childPartitionId: Long,
        dependencies: Array<DependencyResult>,
        overwrite: Boolean
    ): PartitionDependencyResult {
        val saveResult = PartitionDependencyResult()
        try {
            if (overwrite) {
                deleteByChildId(childPartitionId)
            }
            // WARNING: Will fail sql execution if id (meaning relation) already exists
            storeAll(childPartitionId, dependencies)
            saveResult.isSuccess = true
            saveResult.isError = false
        } catch (e: SQLException) {
            saveResult.isSuccess = false
            saveResult.isError = true
            saveResult.exception = e
            saveResult.message = e.message
        }
        return saveResult
    }
}