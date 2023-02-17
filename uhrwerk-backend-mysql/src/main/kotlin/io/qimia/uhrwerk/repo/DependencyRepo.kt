package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import io.qimia.uhrwerk.common.metastore.model.HashKeyUtils
import io.qimia.uhrwerk.repo.RepoUtils.jsonToArray
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

    fun getById(id: Long): DependencyModel? =
        super.find(SELECT_BY_ID, {
            it.setLong(1, id)
        }, this::map)

    fun getByTableId(tableId: Long): List<DependencyModel> =
        super.findAll(SELECT_BY_TABLE_ID, {
            it.setLong(1, tableId)
        }, this::map)

    fun getByTableKey(tableKey: Long): List<DependencyModel> =
        super.findAll(SELECT_BY_TABLE_KEY, {
            it.setLong(1, tableKey)
        }, this::map)

    fun deactivateByTableKey(tableKey: Long): Int? =
        super.update(DEACTIVATE_BY_TABLE_KEY) {
            it.setLong(1, tableKey)
        }

    private fun insertParams(
        dependency: DependencyModel,
        insert: PreparedStatement
    ): PreparedStatement {
        insert.setLong(1, dependency.tableId!!)
        insert.setLong(2, dependency.tableKey!!)
        insert.setLong(3, dependency.dependencyTargetKey!!)
        insert.setLong(4, dependency.dependencyTableKey!!)

        if (dependency.viewName.isNullOrEmpty())
            insert.setNull(5, Types.VARCHAR)
        else
            insert.setString(5, dependency.viewName)

        if (dependency.partitionMappings.isNullOrEmpty())
            insert.setNull(6, Types.VARCHAR)
        else
            insert.setString(6, toJson(dependency.partitionMappings!!))

        if (dependency.dependencyVariables.isNullOrEmpty())
            insert.setNull(7, Types.VARCHAR)
        else
            insert.setString(7, toJson(dependency.dependencyVariables!!))

        insert.setLong(8, HashKeyUtils.dependencyKey(dependency))
        return insert

    }

    private fun map(rs: ResultSet): DependencyModel {
        val dependency = DependencyModel()
        dependency.id = rs.getLong("dep.id")
        dependency.tableId = rs.getLong("dep.table_id")
        dependency.tableKey = rs.getLong("dep.table_key")
        dependency.dependencyTargetKey = rs.getLong("dep.dependency_target_key")
        dependency.dependencyTableKey = rs.getLong("dep.dependency_table_key")
        dependency.area = rs.getString("tab.area")
        dependency.vertical = rs.getString("tab.vertical")
        dependency.tableName = rs.getString("tab.name")
        dependency.viewName = rs.getString("dep.view_name")

        val partitionMappings = rs.getString("dep.partition_mappings")
        if (!partitionMappings.isNullOrEmpty())
            dependency.partitionMappings = jsonToMap(partitionMappings)

        val dependencyVariables = rs.getString("dep.dependency_variables")
        if (!dependencyVariables.isNullOrEmpty())
            dependency.dependencyVariables = jsonToArray(dependencyVariables)

        dependency.version = rs.getString("tab.version")
        dependency.autoLoad = rs.getBoolean("dep.auto_load")
        dependency.format = rs.getString("tar.format")
        dependency.targetDBTableName = rs.getString("tar.table_name")
        dependency.hashKey = rs.getLong("dep.hash_key")

        val deactivatedTs = rs.getTimestamp("dep.deactivated_ts")
        if (deactivatedTs != null)
            dependency.deactivatedTs = deactivatedTs.toLocalDateTime()

        return dependency
    }


    companion object {
        private val INSERT = """
            INSERT INTO DEPENDENCY (table_id,
                                    table_key,
                                    dependency_target_key,
                                    dependency_table_key,
                                    view_name,
                                    partition_mappings,
                                    dependency_variables,
                                    auto_load,
                                    hash_key)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """.trimIndent()

        private val SELECT_BY_ID = """
            SELECT dep.id,
                   dep.table_id,
                   dep.table_key,
                   dep.dependency_target_key,
                   dep.dependency_table_key,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   dep.dependency_variables,
                   tar.format,
                   tar.table_name,
                   tab.version,
                   dep.auto_load,
                   dep.hash_key,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_key = tab.hash_key AND tab.deactivated_ts IS NULL
                     JOIN TARGET tar ON dep.dependency_target_key = tar.hash_key AND tar.deactivated_ts IS NULL
            WHERE dep.id = ?
        """.trimIndent()

        private const val DEACTIVATE_BY_TABLE_KEY =
            "UPDATE DEPENDENCY SET deactivated_ts = CURRENT_TIMESTAMP() WHERE table_key = ? AND deactivated_ts IS NULL"


        private val SELECT_BY_TABLE_ID = """
            SELECT dep.id,
                   dep.table_id,
                   dep.table_key,
                   dep.dependency_target_key,
                   dep.dependency_table_key,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   dep.dependency_variables,
                   tar.format,
                   tar.table_name,
                   tab.version,
                   dep.auto_load,
                   dep.hash_key,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_key = tab.hash_key AND tab.deactivated_ts IS NULL
                     JOIN TARGET tar ON dep.dependency_target_key = tar.hash_key AND tar.deactivated_ts IS NULL
            WHERE dep.table_id = ? 
        """.trimIndent()

        private val SELECT_BY_TABLE_KEY = """
            SELECT dep.id,
                   dep.table_id,
                   dep.table_key,
                   dep.dependency_target_key,
                   dep.dependency_table_key,
                   tab.area,
                   tab.vertical,
                   tab.name,
                   dep.view_name,
                   dep.partition_mappings,
                   dep.dependency_variables,
                   tar.format,
                   tar.table_name,
                   tab.version,
                   dep.auto_load,
                   dep.hash_key,
                   dep.deactivated_ts
            FROM DEPENDENCY dep
                     JOIN TABLE_ tab ON dep.dependency_table_key = tab.hash_key AND tab.deactivated_ts IS NULL
                     JOIN TARGET tar ON dep.dependency_target_id = tar.id AND tar.deactivated_ts IS NULL
            WHERE dep.table_key = ? AND dep.deactivated_ts IS NULL
        """.trimIndent()

    }
}