package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.model.TableModel;

public interface TableService {
    /**
     * Saves a Table into the Metastore.
     * If overwrite is set to false and the object already exists in the metastore and is not precisely equal
     * to the one being saved, it returns a TableResult with error.
     *
     * @param table     Table to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return TableResult.
     */
    TableResult save(TableModel table, boolean overwrite);

    TableModel get(String area, String vertical, String table, String version);
}
