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
        src.connectionId = connectionId
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
    ): TargetModel = TargetModelBuilder()
        .tableId(tableId)
        .connectionId(connectionId)
        .format(format)
        .tableName("dummy_table")
        .build()

    fun target(
        tableId: Long,
        connName: String = "Test-Connection",
        format: String = "parquet"
    ): TargetModel = TargetModelBuilder()
        .tableId(tableId)
        .connection(ConnectionModelBuilder().name(connName).build())
        .format(format)
        .build()

    fun partition(
        targetId: Long,
        partitionTs: LocalDateTime = LocalDateTime.now(),
        partitioned: Boolean = true,
        partitionUnit: PartitionUnit = PartitionUnit.HOURS,
        partitionSize: Int = 1,
    ): Partition =
        PartitionBuilder()
            .targetId(targetId)
            .partitionTs(partitionTs)
            .partitioned(partitioned)
            .partitionUnit(partitionUnit)
            .partitionSize(partitionSize)
            .partitionValues(mapOf("col1" to "val1", "col2" to 20))
            .build()

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
    ): DependencyModel = DependencyModelBuilder()
        .tableId(tableId)
        .dependencyTargetId(dependencyTargetId)
        .dependencyTableId(dependencyTableId)
        .viewName("dummy_view")
        .partitionMappings(
            mapOf(
                "col1" to "col1",
                "col2" to "col2"
            )
        )
        .build()

    fun dependency(
        table: TableModel,
        dependencyTable: TableModel,
        format: String,
        trfType: PartitionTransformType = PartitionTransformType.IDENTITY,
        trfPartSize: Int = 1
    ): DependencyModel = DependencyModelBuilder()
        .tableId(table.id!!)
        .table(table)
        .dependencyTable(dependencyTable)
        .area(dependencyTable.area)
        .vertical(dependencyTable.vertical)
        .tableName(dependencyTable.name)
        .version(dependencyTable.version)
        .format(format)
        .build()

    fun secret(name: String): SecretModel {
        val scr = SecretModel()
        scr.name = name
        scr.type = SecretType.AWS
        scr.awsSecretName = "uhrwerk/meta_store/db_user"
        scr.awsRegion = "eu_west_1"
        return scr
    }

}