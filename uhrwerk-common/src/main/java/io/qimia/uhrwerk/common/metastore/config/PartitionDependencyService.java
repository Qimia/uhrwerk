package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;
import io.qimia.uhrwerk.common.model.Partition;

import java.util.List;

public interface PartitionDependencyService {

  /**
   * Save the one or more partition dependencies for a child partition and a parent-dependency's one
   * or more partitions. An identity-transformation would mean a single partition as parent whereas
   * a aggregate would mean multiple
   *
   * @param childPartitionId id of the child partition
   * @param dependency       DependencyResult object with information about which partitions were used for
   *                         this dependency
   * @param overwrite        overwrite previously found dependencies or not
   * @return PartitionDependencyResult denoting success or what kind of error was generated
   */
  PartitionDependencyResult save(
          Long childPartitionId, DependencyResult dependency, boolean overwrite);

  /**
   * Remove all partition dependencies for a particular child partition and a dependency's
   * DependencyResult
   *
   * @param childPartitionId id of child partition
   * @param dependency       DependencyResult object with information about which partitions were used for
   *                         this dependency
   * @return PartitionDependencyResult denoting success or what kind of error was generated
   */
  PartitionDependencyResult remove(Long childPartitionId, DependencyResult dependency);

  /**
   * Create partition dependencies for all the dependencies of a given child partition
   *
   * @param childPartitionId id of the child partition object
   * @param dependencies     Array of DependencyResult for each of the dependencies
   * @param overwrite        overwrite previously found dependencies or not
   * @return PartitionDependencyResult denoting success or what kind of error was generated
   */
  PartitionDependencyResult saveAll(
          Long childPartitionId, DependencyResult[] dependencies, boolean overwrite);

  /**
   * Remove all partition dependencies for a particular child partition
   *
   * @param childPartitionId id of child partition
   * @return PartitionDependencyResult denoting success or what kind of error was generated
   */
  PartitionDependencyResult removeAll(Long childPartitionId);

  /**
   * Retrieve all parent partition for a given (child) partition
   *
   * @param childPartition a partition with partition dependencies
   * @return
   */
  List<Partition> getParentPartitions(Partition childPartition);
}
