package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Dependency;
import io.qimia.uhrwerk.common.model.Table;

public interface DependencyStoreService {

    /**
     * Save all dependencies for a given table
     *
     * @param table     the table
     * @param overwrite overwrite the previously stored dependencies or not
     * @return DependencyStoreResult object with stored objects, info about success, exceptions and
     * other results
     */
    DependencyStoreResult save(
            Table table,
            boolean overwrite
    );

    /**
     * Retrieve all stored dependencies for a given table
     *
     * @param tableId tableId of the table for which the dependencies are returned
     * @return model Dependency objects
     */
    Dependency[] get(Long tableId);
}
