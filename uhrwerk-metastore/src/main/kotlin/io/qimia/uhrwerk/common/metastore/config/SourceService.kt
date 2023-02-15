package io.qimia.uhrwerk.common.metastore.config

import io.qimia.uhrwerk.common.metastore.model.SourceModel2
import java.sql.SQLException

interface SourceService {
    /**
     * Saves a Source into the Metastore. If overwrite is set to false and the object already exists
     * in the metastore and is not precisely equal to the one being saved, it returns a SourceResult
     * with error.
     *
     * @param source    Source to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return SourceResult.
     */
    fun save(source: SourceModel2, overwrite: Boolean): SourceResult?

    /**
     * Saves several Sources into the Metastore. If overwrite is set to false and the object already
     * exists in the metastore and is not precisely equal to the one being saved, it returns a
     * SourceResult with error. The returned SourceResults can thus be both some successful and with
     * errors, to identify in detail what went wrong where.
     *
     * @param sources   Sources to save.
     * @param overwrite Whether to overwrite non-essential fields.
     * @return An array with SourceResults.
     */
    fun save(sources: List<SourceModel2>, overwrite: Boolean): List<SourceResult>

    /**
     * Returns all sources belonging to a table.
     *
     * @param tableId Table id.
     * @return An array of sources. Could be empty or null when something goes wrong.
     * @throws SQLException         When something goes wrong with the connection to the Metastore.
     * @throws NullPointerException When a source's connection is missing.
     */
    @Throws(SQLException::class, NullPointerException::class)
    fun getByTableId(tableId: Long): List<SourceModel2>?

    fun deactivateByTableKey(tableKey: Long): Int?
}