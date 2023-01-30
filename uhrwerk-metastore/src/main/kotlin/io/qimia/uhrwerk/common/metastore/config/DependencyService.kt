package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.DependencyModel

interface DependencyService {
    /**
     * Save all dependencies for a given table
     *
     * @param tableId    the table ID
     * @param overwrite overwrite the previously stored dependencies or not
     * @return DependencyStoreResult object with stored objects, info about success, exceptions and
     * other results
     */
    fun save(
        tableId: Long,
        dependencies: Array<DependencyModel>?,
        overwrite: Boolean
    ): DependencyStoreResult?

    /**
     * Retrieve all stored dependencies for a given table
     *
     * @param tableId tableId of the table for which the dependencies are returned
     * @return model Dependency objects
     */
    fun getByTableId(tableId: Long): List<DependencyModel>?
    fun deactivateByTableId(tableId: Long): Int?
}