package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Partition;

import java.time.LocalDateTime;

public interface PartitionService {
  /**
   * Saves a Partition into the Metastore. If overwrite is set to false and the object already
   * exists in the metastore and is not precisely equal to the one being saved, it returns a
   * PartitionResult with error.
   *
   * @param partition Partition to save.
   * @param overwrite Whether to overwrite non-essential fields.
   * @return PartitionResult.
   */
  PartitionResult save(Partition partition, boolean overwrite);

  /**
   * Saves several Partitions into the Metastore. If overwrite is set to false and the object
   * already exists in the metastore and is not precisely equal to the one being saved, it returns a
   * PartitionResult with error. The returned PartitionResults can thus be both some successful and
   * with errors, to identify in detail what went wrong where.
   *
   * @param partitions Partitions to save.
   * @param overwrite  Whether to overwrite non-essential fields.
   * @return An array with PartitionResults.
   */
  PartitionResult[] save(Partition[] partitions, boolean overwrite);

  /**
   * Gets all partitions with the specified targetId and partition timestamps.
   *
   * @param targetId    Target ID.
   * @param partitionTs An array of partition timestamps.
   * @return An array of partitions (can be empty when there were no such partitions) or null in
   * case something went wrong.
   */
  Partition[] getPartitions(Long targetId, LocalDateTime[] partitionTs);

  /**
   * Gets the latest partition for a specified target id.
   *
   * @param targetId Target ID.
   * @return The latest partition for this target. Can be null in case something
   * went wrong or there are no partitions.
   */
  Partition getLatestPartition(Long targetId);
}
