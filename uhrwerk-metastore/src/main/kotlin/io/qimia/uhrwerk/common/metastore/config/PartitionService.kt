package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.Partition
import java.time.LocalDateTime

interface PartitionService {
    /**
     * Saves a Partition into the Metastore. If overwrite is set to false and the object already
     * exists in the metastore and is not precisely equal to the one being saved, it returns a
     * PartitionResult with error.
     *
     * @param partition Partition to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return PartitionResult.
     */
    fun save(partition: Partition, overwrite: Boolean): PartitionResult?

    /**
     * Saves several Partitions into the Metastore. If overwrite is set to false and the object
     * already exists in the metastore and is not precisely equal to the one being saved, it returns a
     * PartitionResult with error. The returned PartitionResults can thus be both some successful and
     * with errors, to identify in detail what went wrong where.
     *
     * @param partitions Partitions to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return An array with PartitionResults.
     */
    fun save(partitions: List<Partition>, overwrite: Boolean): List<PartitionResult>

    /**
     * Gets all partitions with the specified targetId and partition timestamps.
     *
     * @param targetId Target ID.
     * @param partitionTs An array of partition timestamps.
     * @return An array of partitions (can be empty when there were no such partitions) or null in
     * case something went wrong.
     */
    fun getPartitions(targetId: Long, partitionTs: List<LocalDateTime>): List<Partition>

    /**
     * Gets the latest partition for a specified target id.
     *
     * @param targetId Target ID.
     * @return The latest partition for this target. Can be null in case something went wrong or there
     * are no partitions.
     */
    fun getLatestPartition(targetId: Long): Partition?
    fun getById(id: Long): Partition?
}