package io.qimia.uhrwerk.integration

import TestUtils.filePath
import com.google.common.truth.Truth
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import org.junit.jupiter.api.Test

class TableReadTests {
    @Test
    fun readSourceTable() {
        val tableFile = filePath("config/uhrwerk_examples/table_category_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)

        Truth.assertThat(table.sources).isNotNull()
        Truth.assertThat(table.sources!!.toList()).isNotEmpty()
        table.sources!!.forEach {
            Truth.assertThat(it.connection).isNotNull()
            Truth.assertThat(it.connection!!.name).isNotNull()
        }


        Truth.assertThat(table.targets).isNotNull()
        Truth.assertThat(table.targets!!.toList()).hasSize(2)

        table.targets!!.forEach {
            Truth.assertThat(it.connection).isNotNull()
            Truth.assertThat(it.connection!!.name).isNotNull()
        }

        Truth.assertThat(table.partitionColumns).isNotNull()
        Truth.assertThat(table.partitionColumns).hasLength(2)
        Truth.assertThat(table.partitionColumns).isEqualTo(
            arrayOf(
                "partition_column1",
                "partition_column1"
            )
        )

        Truth.assertThat(table.tableVariables).isNotNull()
        Truth.assertThat(table.tableVariables).hasLength(2)
        Truth.assertThat(table.tableVariables).isEqualTo(arrayOf("table_variable1", "table_variable2"))

    }

    @Test
    fun tableDependentRead() {
        val tableFile = filePath("config/uhrwerk_examples/table_category_prc_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)

        Truth.assertThat(table.sources).isNull()

        Truth.assertThat(table.targets).isNotNull()
        Truth.assertThat(table.targets!!.toList()).hasSize(2)

        table.targets!!.forEach {
            Truth.assertThat(it.connection).isNotNull()
            Truth.assertThat(it.connection!!.name).isNotNull()
        }

        Truth.assertThat(table.dependencies).isNotNull()
        Truth.assertThat(table.dependencies!!).hasLength(1)
        Truth.assertThat(table.dependencies!![0].partitionMappings).isNotNull()
        Truth.assertThat(table.dependencies!![0].partitionMappings!!).hasSize(2)
        Truth.assertThat(table.dependencies!![0].partitionMappings!!).containsExactlyEntriesIn(
            mapOf(
                "partition_column1" to "\$table_variable1\$",
                "partition_column2" to "\$table_variable2\$"
            )
        )

        Truth.assertThat(table.partitionColumns).isNotNull()
        Truth.assertThat(table.partitionColumns).hasLength(2)
        Truth.assertThat(table.partitionColumns).isEqualTo(
            arrayOf(
                "partition_column1",
                "partition_column1"
            )
        )

        Truth.assertThat(table.tableVariables).isNotNull()
        Truth.assertThat(table.tableVariables).hasLength(2)
        Truth.assertThat(table.tableVariables).isEqualTo(arrayOf("table_variable1", "table_variable2"))

    }
}