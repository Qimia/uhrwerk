package io.qimia.uhrwerk

import io.qimia.uhrwerk.common.metastore.builders.*
import io.qimia.uhrwerk.common.metastore.model.*
import io.qimia.uhrwerk.common.model.*
import java.time.LocalDateTime

object TestData {

    fun table(name: String): TableModel = TableModelBuilder()
        .area("dwh")
        .vertical("vertical1")
        .name(name)
        .version("1.0")
        .partitionSize(1)
        .parallelism(8)
        .maxBulkSize(96)
        .partitioned(true)
        .className(
            listOf(
                "dwh",
                "vertical1",
                name,
                "1.0"
            ).joinToString(separator = ".")
        )
        .partitionUnit(PartitionUnit.HOURS)
        .partitionSize(1)
        .parallelism(8)
        .maxBulkSize(96)
        .partitionColumns(arrayOf("col1", "col2"))
        .build()

    fun connection(name: String): ConnectionModel = ConnectionModelBuilder()
        .name(name)
        .type(ConnectionType.S3)
        .path("ConnectionRepoTest-Save")
        .awsAccessKeyID("access-key-id1")
        .awsSecretAccessKey("secret-access-key1")
        .build()

    fun source2(
        path: String,
        tableId: Long,
        connectionId: Long
    ): SourceModel2 {
        val src = SourceModel2()
        src.tableId = tableId
        src.connectionKey = connectionId
        src.path = path
        src.format = "jdbc"
        src.ingestionMode = IngestionMode.INTERVAL
        src.intervalTempUnit = PartitionUnit.HOURS
        src.intervalTempSize = 2
        src.parallelPartitionNum = 40
        src.autoLoad = false
        src.sourceVariables = arrayOf("var1", "var2")
        return src
    }

    fun source2(
        path: String,
        tableId: Long,
        connection: ConnectionModel
    ): SourceModel2 {
        val src = SourceModel2()
        src.tableId = tableId
        src.connection = connection
        src.path = path
        src.format = "jdbc"
        src.ingestionMode = IngestionMode.INTERVAL
        src.intervalTempUnit = PartitionUnit.HOURS
        src.intervalTempSize = 2
        src.parallelPartitionNum = 40
        src.autoLoad = false
        return src
    }

    fun target(
        tableId: Long,
        connectionId: Long,
        format: String = "parquet"
    ): TargetModel {
        val target = TargetModel()
        target.tableId = tableId
        target.connectionKey = connectionId
        target.format = format
        target.tableName = "dummy_table"
        return target
    }

    fun target(
        tableId: Long,
        connName: String = "Test-Connection",
        format: String = "parquet"
    ): TargetModel {
val target = TargetModel()
        target.tableId = tableId
        target.connection = connection(connName)
        target.format = format
        target.tableName = "dummy_table"
        return target

    }

    fun partition(
        targetId: Long,
        partitionTs: LocalDateTime = LocalDateTime.now(),
        partitioned: Boolean = true,
        partitionUnit: PartitionUnit = PartitionUnit.HOURS,
        partitionSize: Int = 1,
    ): Partition {
        val part = Partition()
        part.targetKey = targetId
        part.partitionTs = partitionTs
        part.partitioned = partitioned
        part.partitionUnit = partitionUnit
        part.partitionSize = partitionSize
        part.partitionValues = mapOf("col1" to "val1", "col2" to 20)
        return part
    }

    fun partitionDependencies(childPartId: Long, depPartIds: List<Long>) =
        depPartIds.map {
            PartitionDependencyBuilder()
                .partitionId(childPartId)
                .dependencyPartitionId(it)
                .build()
        }

    fun timestamps(stHr: Int): List<LocalDateTime> =
        (0..4).map { LocalDateTime.of(2022, 7, 20, stHr + it, 0) }


    fun dependency(
        tableId: Long,
        dependencyTargetId: Long,
        dependencyTableId: Long
    ): DependencyModel {
        val dep = DependencyModel()
        dep.tableId = tableId
        dep.dependencyTargetKey = dependencyTargetId
        dep.dependencyTableKey = dependencyTableId
        dep.viewName = "dummy_view"
        dep.partitionMappings = mapOf("col1" to "col1", "col2" to "col2")
        return dep
    }
    fun dependency(
        table: TableModel,
        dependencyTable: TableModel,
        format: String
    ): DependencyModel {
        val dep = DependencyModel()
        dep.tableId = table.id!!
        dep.tableKey = table.hashKey
        dep.dependencyTargetKey = dependencyTable.targets?.get(0)!!.hashKey
        dep.dependencyTableKey = dependencyTable.hashKey
        dep.viewName = "dummy_view"
        dep.partitionMappings = mapOf("col1" to "col1", "col2" to "col2")
        dep.area = dependencyTable.area
        dep.vertical = dependencyTable.vertical
        dep.tableName = dependencyTable.name
        dep.version = dependencyTable.version
        dep.format = format
        return dep
    }
    fun secret(name: String): SecretModel {
        val scr = SecretModel()
        scr.name = name
        scr.type = SecretType.AWS
        scr.awsSecretName = "uhrwerk/meta_store/db_user"
        scr.awsRegion = "eu_west_1"
        return scr
    }

}