package io.qimia.uhrwerk.backend;

import io.qimia.uhrwerk.config.model.Table;
import io.qimia.uhrwerk.config.model.Target;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public interface DependencyBackend {

    /**
     * Check if dependencies have been met for a group of dependencies and a List of partitions at one time
     * @param batchedDependencies An array of dependencies **with the same partitionSize**
     * @param partitionTimes The list of partitions that need to be checked
     * @param partitionSize The partition size that each of the dependencies use
     * @return
     */
    public List<DependencyResult> checkBatchedDependencies(Dependency[] batchedDependencies, List<LocalDateTime> partitionTimes, Duration partitionSize);

    /**
     * Check for some table and a list of partitions which partitions have not been written
     * @param table Input table for which the targets need to be checked
     * @param partitionTimes All times that the user wants to have available
     * @return List of partitions which still need one or more targets written
     */
    public List<TargetNeeded> checkTargetNeeded(Table table, List<LocalDateTime> partitionTimes);

    public Map<LocalDateTime,List<Dependency,List<Target>>> processingPartitions(Long tableId, LocalDateTime star, LocalDateTime end);

}
