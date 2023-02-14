package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.config.PartitionResult
import io.qimia.uhrwerk.common.metastore.config.PartitionService
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.repo.PartitionRepo
import io.qimia.uhrwerk.repo.RepoUtils.toJson
import java.sql.SQLException
import java.time.LocalDateTime

class PartitionDAO : PartitionService {

    val repo: PartitionRepo = PartitionRepo()


    override fun save(partition: Partition, overwrite: Boolean): PartitionResult {
        val result = PartitionResult()
        try {
            val newPartition = repo.save(partition)
            result.isSuccess = true
            result.newResult = newPartition
        } catch (e: SQLException) {
            result.isError = true
            result.isSuccess = false
            result.exception = e
            result.message = e.message
        } catch (e: NullPointerException) {
            result.isError = true
            result.isSuccess = false
            result.exception = e
            result.message = e.message
        }
        return result
    }

    override fun save(partitions: List<Partition>, overwrite: Boolean): List<PartitionResult> {
        return partitions.map { save(it, overwrite) }
    }

    override fun getPartitions(
        targetId: Long,
        partitionTs: List<LocalDateTime>
    ): List<Partition> = repo.getAllByTargetTs(targetId, partitionTs)

    override fun getById(id: Long): Partition? = repo.getById(id)

    override fun getLatestPartition(targetKey: Long): Partition? =
        repo.getLatestByTargetKey(targetKey)

    override fun getLatestPartitions(
        targetKey: Long,
        partitionValues: Map<String, Any>
    ): List<Partition>? {
        return repo.getLatestByTargetKeyPartitionValues(targetKey, toJson(partitionValues))
    }
}