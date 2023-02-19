package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException

@JsonIgnoreProperties(ignoreUnknown = true)
data class Table(
    var area: String? = null,
    var vertical: String? = null,
    var table: String? = null,
    var version: String? = null,
    @JsonProperty("class_name")
    var className: String? = null,
    @JsonProperty("transform_sql_query")
    var transformSqlQuery: String? = null,
    @JsonProperty("partition_columns")
    var partitionColumns: Array<String>? = null,
    @JsonProperty("partitions")
    var partitionMappings: Array<PartitionMapping>? = null,
    @JsonProperty("variables")
    var tableVariables: Array<String>? = null,
    @JsonProperty("dynamic_partitioning")
    var dynamicPartitioning: Boolean = false,
    var parallelism: Int? = 1,
    @JsonProperty("max_bulk_size")
    var maxBulkSize: Int? = 1,
    var partition: Partition? = null,
    var sources: Array<Source>? = null,
    var targets: Array<Target>? = null,
    var dependencies: Array<Dependency>? = null
) {
    fun validate(path: String) {
        var path = path
        path += "table/"
        if (area == null) {
            throw ConfigException("Missing field: " + path + "area")
        }
        if (vertical == null) {
            throw ConfigException("Missing field: " + path + "vertical")
        }
        if (table == null) {
            throw ConfigException("Missing field: " + path + "table")
        }
        if (version == null) {
            throw ConfigException("Missing field: " + path + "version")
        }
        if (parallelism == null) {
            throw ConfigException("Missing field: " + path + "parallelism")
        }
        if (maxBulkSize == null) {
            throw ConfigException("Missing field: " + path + "max_bulk_size")
        }
        if (partition != null) {
            partition!!.validate(path)
        }
        if (sources == null && dependencies == null) {
            throw ConfigException("Missing field: " + path + "sources or dependencies")
        } else {
            if (dependencies != null) {
                for (d in dependencies!!) {
                    d.validate(path)
                }
            }
            if (sources != null) {
                for (s in sources!!) {
                    s.validate(path)
                }
            }
        }
        if (targets == null) {
            throw ConfigException("Missing field: " + path + "targets")
        } else {
            for (t in targets!!) {
                t.validate(path)
            }
        }

        if (partitionColumns != null && partitionMappings != null) {
            throw ConfigException("Only one of partition_columns or partitions can be set in $path")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Table) return false

        if (area != other.area) return false
        if (vertical != other.vertical) return false
        if (table != other.table) return false
        if (version != other.version) return false
        if (className != other.className) return false
        if (transformSqlQuery != other.transformSqlQuery) return false
        if (parallelism != other.parallelism) return false
        if (maxBulkSize != other.maxBulkSize) return false
        if (partition != other.partition) return false
        if (sources != null) {
            if (other.sources == null) return false
            if (!sources.contentEquals(other.sources)) return false
        } else if (other.sources != null) return false
        if (targets != null) {
            if (other.targets == null) return false
            if (!targets.contentEquals(other.targets)) return false
        } else if (other.targets != null) return false
        if (dependencies != null) {
            if (other.dependencies == null) return false
            if (!dependencies.contentEquals(other.dependencies)) return false
        } else if (other.dependencies != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = area?.hashCode() ?: 0
        result = 31 * result + (vertical?.hashCode() ?: 0)
        result = 31 * result + (table?.hashCode() ?: 0)
        result = 31 * result + (version?.hashCode() ?: 0)
        result = 31 * result + (className?.hashCode() ?: 0)
        result = 31 * result + (transformSqlQuery?.hashCode() ?: 0)
        result = 31 * result + (parallelism ?: 0)
        result = 31 * result + (maxBulkSize ?: 0)
        result = 31 * result + (partition?.hashCode() ?: 0)
        result = 31 * result + (sources?.contentHashCode() ?: 0)
        result = 31 * result + (targets?.contentHashCode() ?: 0)
        result = 31 * result + (dependencies?.contentHashCode() ?: 0)
        return result
    }

    override fun toString(): String {
        return "Table(area=$area, vertical=$vertical, table=$table, version=$version, className=$className, transformSqlQuery=$transformSqlQuery, parallelism=$parallelism, maxBulkSize=$maxBulkSize, partition=$partition, sources=${sources?.contentToString()}, targets=${targets?.contentToString()}, dependencies=${dependencies?.contentToString()})"
    }

}