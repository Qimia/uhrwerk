package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.builders.DependencyModelBuilder
import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.repo.RepoUtils.jsonToMap
import io.qimia.uhrwerk.repo.RepoUtils.toJson
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types

class DependencyRepo : BaseRepo<DependencyModel>() {

    fun save(dependency: DependencyModel): DependencyModel? =
        super.insert(dependency, INSERT) {
            insertParams(dependency, it)
        }

    fun save(dependencies: List<DependencyModel>): List<DependencyModel>? =
        dependencies.mapNotNull { save(it) }

    fun getByHashKey(hashKey: Long): DependencyModel? =
        super.getByHashKey(
            SELECT_BY_HASH_KEY, {
                it.setLong(1, hashKey)
            }, this::map
        )

    fun getById(id: Long): DependencyModel? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getByTableId(tableId: Long): List<DependencyModel> =
        super.findAll(SELECT_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun deactivateByTableId(tableId: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_ID) {
            it.setLong(1, tableId)
        }

    fun deleteById(id: Long): Int? =
        super.update(DELETE_BY_ID) {
            it.setLong(1, id)
        }

    private fun insertParams(
        dependency: DependencyModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setLong(1, dependency.tableId!!)
        insert.setLong(2, dependency.dependencyTargetId!!)
        insert.setLong(3, dependency.dependencyTableId!!)

        if (dependency.viewName.isNullOrEmpty())
            insert.setNull(4, Types.VARCHAR)
        else
            insert.setString(4, dependency.viewName)

        if (dependency.partitionMappings.isNullOrEmpty())
            insert.setNull(5, Types.VARCHAR)
        else
            insert.setString(5, toJson(dependency.partitionMappings!!))

        insert.setLong(6, HashKeyUtils.dependencyKey(dependency))
        return insert

    }

    private fun map(rs: ResultSet): DependencyModel {
        val builder = DependencyModelBuilder()
            .id(rs.getLong("dep.id"))
            .tableId(rs.getLong("dep.table_id"))
            .dependencyTargetId(rs.getLong("dep.dependency_target_id"))
            .dependencyTableId(rs.getLong("dep.dependency_table_id"))
            .tableName(rs.getString("tab.name"))
            .viewName(rs.getString("dep.view_name"))
            .area(rs.getString("tab.area"))
            .vertical(rs.getString("tab.vertical"))
            .version(rs.getString("tab.version"))
            .format(rs.getString("tar.format"))

        val partitionMappings = rs.getString("dep.partition_mappings")
        if (!partitionMappings.isNullOrEmpty())
            builder.partitionMappings(jsonToMap(partitionMappings))

        val deactivatedTs = rs.getTimestamp("dep.deactivated_ts")
        if (deactivatedTs != null)
            builder.deactivatedTs(deactivatedTs.toLocalDateTime())

        return builder.build()
    }

    companion object {
        private val INSERT = """
            INSERT INTO DEPENDENCY (table_id,
                                    dependency_target_id,
                                    dependency_table_id,
                                    view_name,
                                    partition_mappings,
                                    hash_key)
            VALUES (?, ?, ?, ?, ?, ?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT dep.id,
                   dep.table_id,
                   dep.dependency_target_id,
                   dep.dependency_table_id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   tar.format,
                   tab.version,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_id = tab.id
                     JOIN TARGET tar ON dep.dependency_target_id = tar.id
            WHERE dep.id = ?
        """.trimIndent()

        private val SELECT_BY_HASH_KEY = """
            SELECT dep.id,
                   dep.table_id,
                   dep.dependency_target_id,
                   dep.dependency_table_id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   tar.format,
                   tab.version,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_id = tab.id
                     JOIN TARGET tar ON dep.dependency_target_id = tar.id
            WHERE dep.hash_key = ? AND dep.deactivated_ts IS NULL
        """.trimIndent()

        private const val DELETE_BY_ID = "DELETE FROM DEPENDENCY WHERE id = ?"

        private const val DEACTIVATE_BY_TABLE_ID =
            "UPDATE DEPENDENCY SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_id = ?"

        private val SELECT_BY_TABLE_ID = """
            SELECT dep.id,
                   dep.table_id,
                   dep.dependency_target_id,
                   dep.dependency_table_id,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   tar.format,
                   tab.version,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_id = tab.id
                     JOIN TARGET tar ON dep.dependency_target_id = tar.id
            WHERE dep.table_id = ? AND dep.deactivated_ts IS NULL
        """.trimIndent()

    }
}