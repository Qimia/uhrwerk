package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.model.PartitionTransformType
import io.qimia.uhrwerk.common.model.PartitionUnit
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
        val dependencyId = res.getLong(1)
        val targetId = res.getLong(2)
        val tableId = res.getLong(3)
        val specTablePartitionUnit = res.getString(4)
        var partitionUnit: PartitionUnit? = null
        if (specTablePartitionUnit != null) {
            partitionUnit = PartitionUnit.valueOf(specTablePartitionUnit)
        }
        val partitionSize = res.getInt(5)
        val transformType = PartitionTransformType.valueOf(res.getString(6))
        val transformUnitStr = res.getString(7)
        var transformUnit: PartitionUnit? = null
        if (transformUnitStr != null && transformUnitStr != "") {
            transformUnit = PartitionUnit.valueOf(transformUnitStr)
        }
        val transformSize = res.getInt(8)
        val connectionId = res.getLong(9)

        return TablePartition(
            dependencyId,
            targetId,
            tableId,
            partitionUnit,
            partitionSize,
            transformType,
            transformUnit,
            transformSize,
            connectionId
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
            SELECT D.id,
                   TR.id,
                   T.id,
                   T.partition_unit,
                   T.partition_size,
                   D.transform_type,
                   D.transform_partition_unit,
                   D.transform_partition_size,
                   TR.connection_id
            FROM TABLE_ T
                     JOIN (SELECT d.id,
                                  d.dependency_table_id,
                                  d.dependency_target_id,
                                  transform_type,
                                  transform_partition_unit,
                                  transform_partition_size
                           FROM TABLE_ t
                                    JOIN DEPENDENCY d on t.id = d.table_id
                           WHERE t.id = ?) AS D ON D.dependency_table_id = T.id
                     JOIN TARGET TR on TR.id = D.dependency_target_id
            ORDER BY D.id
        """.trimIndent()
    }
}

data class TablePartition(
    val dependencyId: Long,
    val targetId: Long,
    val tableId: Long,
    val partitionUnit: PartitionUnit?,
    val partitionSize: Int,
    val transformType: PartitionTransformType,
    val transformUnit: PartitionUnit?,
    val transformSize: Int,
    val connectionId: Long
)