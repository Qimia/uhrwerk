package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.dependency.DependencyResult;

public interface PartitionDependencyService {

    /**
     * Save the one or more partition dependencies for a child partition and a parent-dependency's one or more partitions.
     * An identity-transformation would mean a single partition as parent whereas a aggregate would mean multiple
     *
     * @param childPartitionId id of the child partition
     * @param dependency       DependencyResult object with information about which partitions were used for this dependency
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    public PartitionDependencyResult save(Long childPartitionId, DependencyResult dependency, boolean overwrite);

    /**
     * Remove all partition dependencies for a particular child partition and a dependency's DependencyResult
     *
     * @param childPartitionId id of child partition
     * @param dependency       DependencyResult object with information about which partitions were used for this dependency
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    public PartitionDependencyResult remove(Long childPartitionId, DependencyResult dependency);

    /**
     * Create partition dependencies for all the dependencies of a given child partition
     *
     * @param childPartitionId id of the child partition object
     * @param dependencies     Array of DependencyResult for each of the dependencies
     * @param overwrite        overwrite previously found dependencies or not
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    public PartitionDependencyResult saveAll(Long childPartitionId, DependencyResult[] dependencies, boolean overwrite);

    /**
     * Remove all partition dependencies for a particular child partition
     *
     * @param childPartitionId id of child partition
     * @return PartitionDependencyResult denoting success or what kind of error was generated
     */
    public PartitionDependencyResult removeAll(Long childPartitionId);
}
