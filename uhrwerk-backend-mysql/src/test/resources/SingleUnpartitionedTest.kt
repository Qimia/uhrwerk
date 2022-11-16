import com.google.common.truth.Truth
import io.qimia.uhrwerk.ConnectionHelper
import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.ConnectionType
import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.common.model.*
import io.qimia.uhrwerk.common.model.TargetModel
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Duration
import java.time.LocalDateTime

class SingleUnpartitionedTest {
    private val insertPartitionQuery = "INSERT INTO PARTITION_(id, target_id, partition_ts, partitioned) VALUES(?,?,?,?)"


    private fun generateConnection(): ConnectionModel {
        val conn = ConnectionModel.builder()
                .name("some_new_conn01")
                .type(ConnectionType.FS)
                .build()
        return conn
    }

    @Test
    @Throws(SQLException::class)
    fun setupTable() {
        val db = ConnectionHelper.getConnecion()

        val connection = generateConnection()
        val connectionDAO = ConnectionDAO()
        connectionDAO.save(connection, true)

        val tableDAO = TableDAO()

        val table = generateTable()
        val target = generateTarget(table, connection)

        table.setTargets(arrayOf(target))

        tableDAO.save(table, true);

        val targetDAO = TargetDAO()
        targetDAO.save(listOf(target), table.id, true)


    }

    private fun generateTarget(table: TableModel, Connection: ConnectionModel): TargetModel {
        val target = TargetModel.builder()
                .tableId(table.id)
                .format("parquet")
                .connection(Connection)
                .build()
        target.setKey()
        return target
    }

    private fun generateTable(): TableModel {
        val tableDepB = TableModel.builder()
                .area("area1")
                .vertical("vertical1")
                .name("name2")
                .version("1.0")
                .partitioned(false)
                .build()
        tableDepB.setKey()
        return tableDepB
    }

    @Test
    internal fun connection() {
        val conn = generateConnection()
        val db = ConnectionHelper.getConnecion()
        val dao = ConnectionDAO()
        val result = dao.save(conn, true)
        Truth.assertThat(result.isSuccess).isNotNull()
        Truth.assertThat(result.isSuccess).isTrue()

    }

    //@Test
    @Throws(SQLException::class)
    fun checkSingleUnpartitionedDependency() {

        //setupTable()
        val connection = generateConnection()
        val db = ConnectionHelper.getConnecion()

        // Setup simple table with a single dependency
        val tableOut = TableModel.builder().build()
        tableOut.area = "area1"
        tableOut.vertical = "vertical1"
        tableOut.name = "tableout"
        tableOut.version = "1.0"
        tableOut.isPartitioned = false
        tableOut.setKey()

        val targetOut = Target()
        targetOut.format = "csv"
        targetOut.tableId = tableOut.id
        targetOut.connection = connection
        targetOut.setKey()

        tableOut.targets = arrayOf(targetOut)

        val tableStm = db.createStatement()

        tableStm.executeUpdate(
                "INSERT INTO TABLE_(id, area, vertical, name, version, parallelism, max_bulk_size, class_name)"
                        + "VALUES ("
                        + tableOut.id
                        + ", 'area1', 'vertical1', 'tableout', '1.0', 1, 1, 'area1.vertical1.tableout.1.0')")

        tableStm.close()

        val tarStm = db.createStatement()
        tarStm.executeUpdate(
                "INSERT INTO TARGET (id, table_id, connection_id, format)"
                        + "VALUES ("
                        + targetOut.id
                        + ","
                        + tableOut.id
                        + ","
                        + connection.id
                        + ", 'csv')")
        tarStm.close()

        // Normally it should be able to run
        val dao = TableDAO()
        val requestTime1 = LocalDateTime.now()
        var resultSet = dao.processingPartitions(tableOut, listOf(requestTime1))

        Assertions.assertEquals(1, resultSet.resolved.size)
        Assertions.assertNull(resultSet.failed)

        // Now add a partition for tableOut
        val partitionlessTs = LocalDateTime.now()
        val outPartition = Partition.builder().build()
        outPartition.targetId = targetOut.id
        outPartition.isPartitioned = false
        outPartition.partitionTs = partitionlessTs
        outPartition.setKey()
        val partStm = db!!.prepareStatement(insertPartitionQuery)
        partStm.setLong(1, outPartition.id)
        partStm.setLong(2, outPartition.targetId)
        partStm.setTimestamp(3, Timestamp.valueOf(outPartition.partitionTs))
        partStm.setBoolean(4, outPartition.isPartitioned)
        partStm.executeUpdate()

        // Now we request to run again within the 60 seconds
        val withinTime = partitionlessTs.plus(Duration.ofSeconds(10))
        resultSet = dao.processingPartitions(tableOut, listOf(withinTime))
        Assertions.assertNull(resultSet.failed)
        Assertions.assertNull(resultSet.resolved)
        Assertions.assertEquals(1, resultSet.processed.size)

        // And if we run outside of the 60 seconds
        val outsideTime = partitionlessTs.plus(Duration.ofMinutes(5))
        resultSet = dao.processingPartitions(tableOut, listOf(outsideTime))
        Assertions.assertNull(resultSet.failed)
        Assertions.assertNull(resultSet.processed)
        Assertions.assertEquals(1, resultSet.resolved.size)
    }
}