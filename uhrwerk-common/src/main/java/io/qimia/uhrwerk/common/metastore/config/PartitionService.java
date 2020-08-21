package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Partition;

public interface PartitionService {
    /**
     * Saves a Partition into the Metastore.
     * If overwrite is set to false and the object already exists in the metastore and is not precisely equal
     * to the one being saved, it returns a PartitionResult with error.
     *
     * @param partition Partition to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return PartitionResult.
     */
    PartitionResult save(Partition partition, boolean overwrite);

    /**
     * Saves several Partitions into the Metastore.
     * If overwrite is set to false and the object already exists in the metastore and is not precisely equal
     * to the one being saved, it returns a PartitionResult with error.
     * The returned PartitionResults can thus be both some successful and with errors, to identify in detail what
     * went wrong where.
     *
     * @param partitions Partitions to save.
     * @param overwrite  Whether to overwrite non-essential fields.
     * @return An array with PartitionResults.
     */
    PartitionResult[] save(Partition[] partitions, boolean overwrite);
}
