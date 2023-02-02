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
    fun getTablePartitions(tableId: Long): List<TablePartition> =
        findAll(SELECT_TABLE_PARTITIONS, {
            it.setLong(1, tableId)
        }, this::map)


    private fun map(res: ResultSet): TablePartition {
        val dependencyId = res.getLong("dep.id")
        val targetId = res.getLong("trg.id")
        val tableId = res.getLong("tab.id")

        val specTablePartitionUnit = res.getString("tab.partition_unit")
        var partitionUnit: PartitionUnit? = null
        if (specTablePartitionUnit != null) {
            partitionUnit = PartitionUnit.valueOf(specTablePartitionUnit)
        }

        val partitionSize = res.getInt("tab.partition_size")

        val connectionId = res.getLong("trg.connection_id")

        val partColumns = res.getString("tab.partition_columns")

        var partitionColumns: Array<String>? = null
        if (!partColumns.isNullOrEmpty())
            partitionColumns = RepoUtils.jsonToArray(partColumns)


        val partMappings = res.getString("dep.partition_mappings")
        var partitionMappings: Map<String, Any>? = null
        if (!partMappings.isNullOrEmpty())
            partitionMappings = RepoUtils.jsonToMap(partMappings)


        return TablePartition(
            dependencyId,
            targetId,
            tableId,
            partitionUnit,
            partitionSize,
            null,
            null,
            null,
            connectionId,
            partitionColumns,
            partitionMappings
        )
    }

    @Throws(SQLException::class)
    private fun findAll(
        query: String,
        setParam: (PreparedStatement) -> Unit,
        map: (res: ResultSet) -> TablePartition
    ): List<TablePartition> {
        val connection = HikariCPDataSource.connection
        connection.use {
            val select = connection.prepareStatement(query)
            setParam(select)
            select.use {
                val res = select.executeQuery()
                res.use {
                    val entities = mutableListOf<TablePartition>()
                    while (res.next()) entities.add(map(res))
                    return entities
                }
            }
        }
    }

    companion object {
        private val SELECT_TABLE_PARTITIONS = """
            SELECT dep.id,
                   trg.id,
                   tab.id,
                   tab.partition_unit,
                   tab.partition_size,
                   trg.connection_id,
                   tab.partition_columns,
                   dep.partition_mappings
            FROM TABLE_ tab
                     JOIN (SELECT d.id,
                                  d.dependency_table_id,
                                  d.dependency_target_id,
                                  d.partition_mappings
                           FROM TABLE_ t
                                    JOIN DEPENDENCY d ON t.id = d.table_id
                           WHERE t.id = ?) AS dep
                          ON dep.dependency_table_id = tab.id
                     JOIN TARGET trg ON trg.id = dep.dependency_target_id
            ORDER BY dep.id
        """.trimIndent()
    }
}

data class TablePartition(
    val dependencyId: Long,
    val targetId: Long,
    val tableId: Long,
    val partitionUnit: PartitionUnit?,
    val partitionSize: Int,
    val transformType: PartitionTransformType?,
    val transformUnit: PartitionUnit?,
    val transformSize: Int?,
    val connectionId: Long,
    val partitionColumns: Array<String>? = null,
    val partitionMappings: Map<String, Any>? = null
)