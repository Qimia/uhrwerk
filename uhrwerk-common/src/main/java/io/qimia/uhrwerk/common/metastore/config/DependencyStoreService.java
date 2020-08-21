package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.PartitionUnit;

public interface DependencyStoreService {

    /**
     * Save all dependencies for a given table
     * @param dependencies All dependencies for a given table
     * @param tableId the table id for that table
     * @param partitionUnit partition unit for checking the dependant's table partition unit
     * @param partitionSize partition size for checking the dependant's table partition size
     * @param overwrite overwrite the previously stored dependencies or not
     * @return DependencyStoreResult object with stored objects, info about success, exceptions and other results
     */
    public DependencyStoreResult save(
            Dependency[] dependencies,
            Long tableId,
            PartitionUnit partitionUnit,
            int partitionSize,
            boolean overwrite
    );

    /**
     * Retrieve all stored dependencies for a given table
     * @param tableId tableId of the table for which the dependencies are returned
     * @return model Dependency objects
     */
    public Dependency[] get(Long tableId);
}
