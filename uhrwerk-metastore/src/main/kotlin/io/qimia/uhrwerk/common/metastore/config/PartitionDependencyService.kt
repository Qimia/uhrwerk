package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult

interface PartitionDependencyService {
    /**
     * Create partition dependencies for all the dependencies of a given child partition
     *
     * @param childPartitionId id of the child partition object
     * @param dependencies     Array of DependencyResult for each of the dependencies
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    fun saveAll(
        childPartitionId: Long, dependencies: Array<DependencyResult>, overwrite: Boolean
    ): PartitionDependencyResult
}