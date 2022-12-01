package io.qimia.uhrwerk.common.metastore.model

import io.qimia.uhrwerk.common.model.TargetModel
import net.openhft.hashing.LongHashFunction

object HashKeyUtils {
    fun targetKey(target: TargetModel): Long {
        assert(target.tableId != null) { "Target.tableId can't be null" }
        assert(target.connectionId != null) { "Target.connectionId can't be null" }
        assert(target.format != null && !target.format!!.isEmpty()) { "Target.format can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(target.tableId)
                .append(target.connectionId)
                .append(target.format)
        )
    }

    fun tableKey(table: TableModel): Long {
        assert(table.area != null && !table.area!!.isEmpty()) { "Table.area can't be null or empty" }
        assert(table.vertical != null && !table.vertical!!.isEmpty()) { "Table.vertical can't be null or empty" }
        assert(table.name != null && !table.name!!.isEmpty()) { "Table.name can't be null or empty" }
        assert(table.version != null && !table.version!!.isEmpty()) { "Table.version can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(table.area)
                .append(table.vertical)
                .append(table.name)
                .append(table.version)
        )
    }

    fun tableKey(dependency: DependencyModel): Long {
        assert(dependency.area != null && !dependency.area!!.isEmpty()) { "Dependency.area can't be null or empty" }
        assert(dependency.vertical != null && !dependency.vertical!!.isEmpty()) { "Dependency.vertical can't be null or empty" }
        assert(dependency.tableName != null && !dependency.tableName!!.isEmpty()) { "Dependency.tableName can't be null or empty" }
        assert(dependency.version != null && !dependency.version!!.isEmpty()) { "Dependency.version can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(dependency.area)
                .append(dependency.vertical)
                .append(dependency.tableName)
                .append(dependency.version)
        )
    }

    fun sourceKey(source: SourceModel): Long {
        assert(source.tableId != null) { "Source.tableId can't be null" }
        assert(source.connectionId != null) { "Source.connectionId can't be null" }
        assert(source.path != null && source.path!!.isNotEmpty()) { "Source.path can't be null or empty" }
        assert(source.format != null && source.format!!.isNotEmpty()) { "Source.format can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(source.tableId)
                .append(source.connectionId)
                .append(source.path)
                .append(source.format)
        )
    }

    fun sourceKey(source: SourceModel2): Long {
        assert(source.tableId != null) { "Source.tableId can't be null" }
        assert(source.connectionId != null) { "Source.connectionId can't be null" }
        assert(source.path != null && source.path!!.isNotEmpty()) { "Source.path can't be null or empty" }
        assert(source.format != null && source.format!!.isNotEmpty()) { "Source.format can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(source.tableId)
                .append(source.connectionId)
                .append(source.path)
                .append(source.format)
        )
    }
    fun dependencyKey(dependency: DependencyModel): Long {
        assert(dependency.tableId != null) { "Dependency.tableId can't be null" }
        assert(dependency.dependencyTargetId != null) { "Dependency.dependencyTargetId can't be null" }
        assert(dependency.dependencyTableId != null) { "Dependency.dependencyTableId can't be null" }
        return hashKey(
            StringBuilder()
                .append(dependency.tableId)
                .append(dependency.dependencyTargetId)
                .append(dependency.dependencyTableId)
        )
    }

    fun connectionKey(connection: ConnectionModel): Long {
        assert(connection.name != null && connection.name!!.isNotEmpty()) { "Connection.name can't be null or empty" }
        return hashKey(StringBuilder().append(connection.name))
    }

    fun secretKey(secret: SecretModel): Long {
        assert(secret.name != null && secret.name!!.isNotEmpty()) { "Secret.name can't be null or empty" }
        return hashKey(StringBuilder().append(secret.name))
    }

    fun secretKey(name: String): Long {
        assert(name != null && name!!.isNotEmpty()) { "Secret.name can't be null or empty" }
        return hashKey(StringBuilder().append(name))
    }


    private fun hashKey(strBuilder: StringBuilder): Long {
        return LongHashFunction.xx().hashChars(strBuilder)
    }
}