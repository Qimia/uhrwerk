package io.qimia.uhrwerk.integration

import TestUtils.filePath
import com.google.common.truth.Truth.assertThat
import io.qimia.uhrwerk.config.builders.YamlConfigReader
import org.junit.jupiter.api.Test

class TableReadTests {
    @Test
    fun readSourceTable() {
        val tableFile = filePath("config/uhrwerk_examples/table_category_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)

        assertThat(table.sources).isNotNull()
        assertThat(table.sources!!.toList()).isNotEmpty()
        table.sources!!.forEach {
            assertThat(it.connection).isNotNull()
            assertThat(it.connection!!.name).isNotNull()
        }


        assertThat(table.targets).isNotNull()
        assertThat(table.targets!!.toList()).hasSize(2)

        table.targets!!.forEach {
            assertThat(it.connection).isNotNull()
            assertThat(it.connection!!.name).isNotNull()
        }

        assertThat(table.partitionColumns).isNotNull()
        assertThat(table.partitionColumns).hasLength(2)
        assertThat(table.partitionColumns).isEqualTo(
            arrayOf(
                "partition_column1",
                "partition_column1"
            )
        )

        assertThat(table.tableVariables).isNotNull()
        assertThat(table.tableVariables).hasLength(4)
        assertThat(table.tableVariables!!).isEqualTo(
            arrayOf(
                "source_variable1",
                "source_variable2",
                "table_variable1",
                "table_variable2"
            )
        )

        assertThat(table.dependencies).isNull()

    }

    @Test
    fun tableDependentRead() {
        val tableFile = filePath("config/uhrwerk_examples/table_category_prc_1.0.yml")
        val table = YamlConfigReader().readTable(tableFile)

        assertThat(table.sources).isNull()

        assertThat(table.targets).isNotNull()
        assertThat(table.targets!!.toList()).hasSize(2)

        table.targets!!.forEach {
            assertThat(it.connection).isNotNull()
            assertThat(it.connection!!.name).isNotNull()
        }

        assertThat(table.dependencies).isNotNull()
        assertThat(table.dependencies!!).hasLength(2)
        assertThat(table.dependencies!![0].partitionMappings).isNotNull()
        assertThat(table.dependencies!![0].partitionMappings!!).hasSize(2)
        assertThat(table.dependencies!![0].partitionMappings!!).containsExactlyEntriesIn(
            mapOf(
                "partition_column1" to "\$dependency_variable1\$",
                "partition_column2" to "\$dependency_variable2\$"
            )
        )

        assertThat(table.dependencies!![0].dependencyVariables).isNotNull()
        assertThat(table.dependencies!![0].dependencyVariables!!).hasLength(2)
        assertThat(table.dependencies!![0].dependencyVariables!!).isEqualTo(
            arrayOf(
                "dependency_variable1",
                "dependency_variable2"
            )
        )

        assertThat(table.partitionColumns).isNotNull()
        assertThat(table.partitionColumns).hasLength(2)
        assertThat(table.partitionColumns).isEqualTo(
            arrayOf(
                "partition_column1",
                "partition_column1"
            )
        )

        assertThat(table.tableVariables).isNotNull()
        assertThat(table.tableVariables).hasLength(6)
        assertThat(table.tableVariables)
            .isEqualTo(
                arrayOf(
                    "dependency_variable1",
                    "dependency_variable2",
                    "dependency_variable3",
                    "table_variable1",
                    "table_variable2",
                    "table_variable3"
                )
            )

        assertThat(table.functions).isNotNull()
        assertThat(table.functions).hasLength(1)
        assertThat(table.functions!![0].functionName).isEqualTo("my_lookup_class_func")

    }
}