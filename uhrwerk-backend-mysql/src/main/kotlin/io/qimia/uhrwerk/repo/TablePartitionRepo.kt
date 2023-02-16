package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.PartitionTransformType
import io.qimia.uhrwerk.common.metastore.model.PartitionUnit
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

class TablePartitionRepo() {
    /**
     * Find the specifications for each of the dependencies for a table
     *
     * @param table child-table for which the specifications need to be found
     * @return List of specifications containing partitioning + transform info per dependency('s
     * table)
     * @throws SQLException
     */
    @Throws(SQLException::class)
    fun getTableDependencySpecs(tableId: Long): List<TableDependencySpec> =
        findAll(SELECT_TABLE_DEP_SPECS, {
            it.setLong(1, tableId)
        }, this::map)


    private fun map(res: ResultSet): TableDependencySpec {
        val dependencyId = res.getLong("dep.id")
        val targetKey = res.getLong("trg.hash_key")

        val specTablePartitionUnit = res.getString("tab.partition_unit")
        var partitionUnit: PartitionUnit? = null
        if (!specTablePartitionUnit.isNullOrEmpty()) {
            partitionUnit = PartitionUnit.valueOf(specTablePartitionUnit)
        }

        val partitionSize = res.getInt("tab.partition_size")

        val connectionKey = res.getLong("trg.connection_key")

        val partColumns = res.getString("tab.partition_columns")

        var partitionColumns: Array<String>? = null
        if (!partColumns.isNullOrEmpty())
            partitionColumns = RepoUtils.jsonToArray(partColumns)


        val partMappings = res.getString("dep.partition_mappings")
        var partitionMappings: Map<String, Any>? = null
        if (!partMappings.isNullOrEmpty())
            partitionMappings = RepoUtils.jsonToMap(partMappings)


        return TableDependencySpec(
            dependencyId = dependencyId,
            targetKey = targetKey,
            connectionKey = connectionKey,
            partitionSize = partitionSize,
            transformSize = null,
            partitionUnit = partitionUnit,
            transformType = null,
            transformUnit = null,
            partitionColumns = partitionColumns,
            partitionMappings = partitionMappings
        )
    }

    @Throws(SQLException::class)
    private fun findAll(
        query: String,
        setParam: (PreparedStatement) -> Unit,
        map: (res: ResultSet) -> TableDependencySpec
    ): List<TableDependencySpec> {
        val connection = HikariCPDataSource.connection
        connection.use {
            val select = connection.prepareStatement(query)
            setParam(select)
            select.use {
                val res = select.executeQuery()
                res.use {
                    val entities = mutableListOf<TableDependencySpec>()
                    while (res.next()) entities.add(map(res))
                    return entities
                }
            }
        }
    }

    companion object {
        private val SELECT_TABLE_DEP_SPECS = """
            SELECT dep.id,
                   trg.hash_key,
                   trg.connection_key,
                   tab.partition_unit,
                   tab.partition_size,
                   tab.partition_columns,
                   dep.partition_mappings
            FROM TABLE_ tab
                     JOIN (SELECT d.id,
                                  d.dependency_target_key,
                                  d.dependency_table_key,
                                  d.partition_mappings
                           FROM TABLE_ t
                                    JOIN DEPENDENCY d ON t.id = d.table_id
                           WHERE t.id = ?) AS dep
                          ON dep.dependency_table_key = tab.hash_key AND tab.deactivated_ts IS NULL
                     JOIN TARGET trg ON trg.hash_key = dep.dependency_target_key AND trg.deactivated_ts IS NULL
            ORDER BY dep.id
        """.trimIndent()
    }
}

data class TableDependencySpec(
    val dependencyId: Long,
    val targetKey: Long,
    val connectionKey: Long,
    val partitionSize: Int,
    val transformSize: Int?,
    val partitionUnit: PartitionUnit?,
    val transformType: PartitionTransformType?,
    val transformUnit: PartitionUnit?,
    val partitionColumns: Array<String>? = null,
    val partitionMappings: Map<String, Any>? = null
)