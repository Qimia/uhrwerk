package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.model.TargetModel
import java.util.*

interface TargetService {
    /**
     * Save an array of targets for a particular table
     *
     * @param targets   all targets for some table
     * @param tableId   id of the table
     * @param overwrite are overwrites allowed or not
     * @return result object showing what is stored, exceptions, if save was successful, and if not, why
     */
    fun save(
        targets: List<TargetModel>,
        tableKey: Long,
        overwrite: Boolean
    ): TargetResult?

    /**
     * Retrieve a target by target-id
     *
     * @param id id (can be generated by Target)
     * @return Optional with Target if found and empty in all other cases
     */
    operator fun get(id: Long): Optional<TargetModel>?

    /**
     * Retrieve all targets set for a table
     *
     * @param tableId id of table for which the targets are defined
     * @return array of Target model objects
     */
    fun getByTableId(tableId: Long): List<TargetModel>?
    fun deactivateByTableKey(tableKey: Long): Int?
}