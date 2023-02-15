package io.qimia.uhrwerk.common.metastore.model

import io.qimia.uhrwerk.common.model.TargetModel
import net.openhft.hashing.LongHashFunction

object HashKeyUtils {

    fun tableKey(table: TableModel): Long {
        assert(!table.area.isNullOrEmpty()) { "Table.area can't be null or empty" }
        assert(!table.vertical.isNullOrEmpty()) { "Table.vertical can't be null or empty" }
        assert(!table.name.isNullOrEmpty()) { "Table.name can't be null or empty" }
        assert(!table.version.isNullOrEmpty()) { "Table.version can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(table.area)
                .append(table.vertical)
                .append(table.name)
                .append(table.version)
        )
    }

    fun tableKey(dependency: DependencyModel): Long {
        assert(!dependency.area.isNullOrEmpty()) { "Dependency.area can't be null or empty" }
        assert(!dependency.vertical.isNullOrEmpty()) { "Dependency.vertical can't be null or empty" }
        assert(!dependency.tableName.isNullOrEmpty()) { "Dependency.tableName can't be null or empty" }
        assert(!dependency.version.isNullOrEmpty()) { "Dependency.version can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(dependency.area)
                .append(dependency.vertical)
                .append(dependency.tableName)
                .append(dependency.version)
        )
    }

    fun targetKey(target: TargetModel): Long {
        assert(target.tableKey != null) { "Target.tableKey can't be null" }
        assert(target.connectionKey != null) { "Target.connectionKey can't be null" }
        assert(!target.format.isNullOrEmpty()) { "Target.format can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(target.tableKey)
                .append(target.connectionKey)
                .append(target.format)
        )
    }


    fun tableKey(area: String?, vertical: String?, tableName: String?, version: String?): Long {
        assert(!area.isNullOrEmpty()) { "area can't be null or empty" }
        assert(!vertical.isNullOrEmpty()) { "vertical can't be null or empty" }
        assert(!tableName.isNullOrEmpty()) { "tableName can't be null or empty" }
        assert(!version.isNullOrEmpty()) { "version can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(area)
                .append(vertical)
                .append(tableName)
                .append(version)
        )
    }

    fun sourceKey(source: SourceModel2): Long {
        assert(source.tableKey != null) { "Source.tableKey can't be null" }
        assert(source.connectionKey != null) { "Source.connectionKey can't be null" }
        assert(!source.path.isNullOrEmpty()) { "Source.path can't be null or empty" }
        assert(!source.format.isNullOrEmpty()) { "Source.format can't be null or empty" }
        return hashKey(
            StringBuilder()
                .append(source.tableKey)
                .append(source.connectionKey)
                .append(source.path)
                .append(source.format)
        )
    }

    fun dependencyKey(dependency: DependencyModel): Long {
        assert(dependency.tableKey != null) { "Dependency.tableKey can't be null" }
        assert(dependency.dependencyTargetKey != null) { "Dependency.dependencyTargetKey can't be null" }
        assert(dependency.dependencyTableKey != null) { "Dependency.dependencyTableKey can't be null" }
        return hashKey(
            StringBuilder()
                .append(dependency.tableKey)
                .append(dependency.dependencyTargetKey)
                .append(dependency.dependencyTableKey)
        )
    }

    fun connectionKey(connection: ConnectionModel): Long {
        assert(!connection.name.isNullOrEmpty()) { "Connection.name can't be null or empty" }
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
        return LongHashFunction.xx128low().hashChars(strBuilder)
    }
}