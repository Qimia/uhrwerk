package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.metastore.model.SourceModel2;
import io.qimia.uhrwerk.common.metastore.model.TableModel;

import java.sql.SQLException;
import java.util.List;

public interface SourceService {
    /**
     * Saves a Source into the Metastore.
     * If overwrite is set to false and the object already exists in the metastore and is not precisely equal
     * to the one being saved, it returns a SourceResult with error.
     *
     * @param source    Source to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return SourceResult.
     */
    SourceResult save(SourceModel2 source, TableModel table, boolean overwrite);

    /**
     * Saves several Sources into the Metastore.
     * If overwrite is set to false and the object already exists in the metastore and is not precisely equal
     * to the one being saved, it returns a SourceResult with error.
     * The returned SourceResults can thus be both some successful and with errors, to identify in detail what
     * went wrong where.
     *
     * @param sources   Sources to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return An array with SourceResults.
     */
    List<SourceResult> save(List<SourceModel2> sources, TableModel table, boolean overwrite);

    /**
     * Returns all sources belonging to a table.
     *
     * @param tableId Table id.
     * @return An array of sources. Could be empty or null when something goes wrong.
     * @throws SQLException         When something goes wrong with the connection to the Metastore.
     * @throws NullPointerException When a source's connection is missing.
     */
    List<SourceModel2> getSourcesByTableId(Long tableId) throws SQLException, NullPointerException;

}
