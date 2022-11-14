package io.qimia.uhrwerk

import io.qimia.uhrwerk.common.model.*
import java.time.LocalDateTime

object TestData {

    fun table(name: String): TableModel = TableModel.builder()
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
        .build()

    fun connection(name: String): ConnectionModel = ConnectionModel.builder()
        .name(name)
        .type(ConnectionType.S3)
        .path("ConnectionRepoTest-Save")
        .awsAccessKeyID("access-key-id1")
        .awsSecretAccessKey("secret-access-key1")
        .build()

    fun source(
        path: String,
        tableId: Long,
        connectionId: Long
    ): SourceModel = SourceModel.builder()
        .tableId(tableId)
        .connectionId(connectionId)
        .path(path)
        .format("jdbc")
        .partitionUnit(PartitionUnit.HOURS)
        .partitionSize(1)
        .parallelLoadNum(40)
        .partitioned(true)
        .autoLoad(false).build()

    fun source(
        path: String,
        tableId: Long,
        connection: ConnectionModel
    ): SourceModel = SourceModel.builder()
        .tableId(tableId)
        .connection(connection)
        .path(path)
        .format("jdbc")
        .partitionUnit(PartitionUnit.HOURS)
        .partitionSize(1)
        .parallelLoadNum(40)
        .partitioned(true)
        .autoLoad(false).build()

    fun target(
        tableId: Long,
        connectionId: Long,
        format: String = "parquet"
    ): TargetModel = TargetModel.builder()
        .tableId(tableId)
        .connectionId(connectionId)
        .format(format)
        .build()

    fun target(
        tableId: Long,
        connName: String = "Test-Connection",
        format: String = "parquet"
    ): TargetModel = TargetModel.builder()
        .tableId(tableId)
        .connection(ConnectionModel.builder().name(connName).build())
        .format(format)
        .build()

    fun partition(
        targetId: Long,
        partitionTs: LocalDateTime = LocalDateTime.now(),
        partitioned: Boolean = true,
        partitionUnit: PartitionUnit = PartitionUnit.HOURS,
        partitionSize: Int = 1,
    ): Partition =
        Partition.builder()
            .targetId(targetId)
            .partitionTs(partitionTs)
            .partitioned(partitioned)
            .partitionUnit(partitionUnit)
            .partitionSize(partitionSize)
            .build()

    fun partitionDependencies(childPartId: Long, depPartIds: List<Long>) =
        depPartIds.map {
            PartitionDependency.builder()
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
    ): DependencyModel = DependencyModel.builder()
        .tableId(tableId)
        .dependencyTargetId(dependencyTargetId)
        .dependencyTableId(dependencyTableId)
        .transformType(PartitionTransformType.IDENTITY)
        .build()

    fun dependency(
        table: TableModel,
        dependencyTable: TableModel,
        format: String,
        trfType: PartitionTransformType = PartitionTransformType.IDENTITY,
        trfPartSize: Int = 1
    ): DependencyModel = DependencyModel.builder()
        .tableId(table.id)
        .table(table)
        .dependencyTable(dependencyTable)
        .area(dependencyTable.area)
        .vertical(dependencyTable.vertical)
        .tableName(dependencyTable.name)
        .version(dependencyTable.version)
        .format(format)
        .transformType(trfType)
        .transformPartitionSize(trfPartSize)
        .build()

}