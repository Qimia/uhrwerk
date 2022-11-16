package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.builders.PartitionDependencyBuilder
import io.qimia.uhrwerk.common.metastore.model.PartitionDependency
import java.sql.PreparedStatement
import java.sql.ResultSet

class PartitionDependencyRepo : BaseRepo<PartitionDependency>() {

    fun save(partitionDependency: PartitionDependency): PartitionDependency? =
        super.insert(partitionDependency, INSERT) {
            insertParams(partitionDependency, it)
        }

    fun save(partitionDependencies: List<PartitionDependency>): List<PartitionDependency>? =
        partitionDependencies.map { save(it)!! }

    fun getById(id: Long): PartitionDependency? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun deleteByChildId(partitionId: Long): Int? =
        super.update(DELETE_PARTDEP_BY_PART_ID) {
            it.setLong(1, partitionId)
        }


    private fun insertParams(
        entity: PartitionDependency,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setLong(1, entity.partitionId!!)
        insert.setLong(2, entity.dependencyPartitionId!!)
        return insert
    }

    private fun map(res: ResultSet): PartitionDependency {
        return PartitionDependencyBuilder()
            .id(res.getLong(1))
            .partitionId(res.getLong(2))
            .dependencyPartitionId(res.getLong(3))
            .build()
    }


    companion object {
        private const val INSERT =
            "INSERT INTO PARTITION_DEPENDENCY (partition_id, dependency_partition_id) VALUES (?, ?)"

        private val SELECT_BY_ID = """
            SELECT id,
                   partition_id,
                   dependency_partition_id
            FROM PARTITION_DEPENDENCY
            WHERE id = ?
        """.trimIndent()


        private const val DELETE_PARTDEP_BY_PART_ID =
            "DELETE FROM PARTITION_DEPENDENCY WHERE partition_id = ?"
    }
}