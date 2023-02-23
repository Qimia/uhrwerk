package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty
import io.qimia.uhrwerk.config.builders.ConfigException
import java.util.*

data class Source(
    @JsonProperty("connection_name")
    var connectionName: String? = null,
    var path: String? = null,
    var format: String? = null,
    var version: String? = null,
    var partition: Partition? = null,
    @JsonProperty("parallel_load")
    var parallelLoad: ParallelLoad? = null,
    var select: Select? = null,
    @JsonProperty("auto_load")
    var autoLoad: Boolean = true,
    @JsonProperty("ingestion_mode")
    var ingestionMode: IngestionMode = IngestionMode.ALL,
    var view: String? = null,
    @JsonProperty("fetch_size")
    var fetchSize: Int = 100000,
) {
    fun validate(path: String) {
        var path = path
        path += "source/"
        if (connectionName == null) {
            throw ConfigException("Missing field: " + path + "connection_name")
        }
        if (this.path == null) {
            throw ConfigException("Missing field: " + path + "path")
        }
        if (format == null) {
            throw ConfigException("Missing field: " + path + "format")
        }
        if (!Arrays.asList(
                "json",
                "parquet",
                "jdbc",
                "orc",
                "libsvm",
                "csv",
                "text",
                "avro",
                "redshift"
            )
                .contains(format)
        ) {
            throw ConfigException("Wrong format! '" + format + "' is not allowed in " + path + "format")
        }
        if (version == null) {
            throw ConfigException("Missing field: " + path + "version")
        }
        if (partition == null) {
            if (select != null) {
                select!!.validateUnpartitioned(path)
            }
        } else {
            partition!!.validate(path)
            if (select == null) {
                throw ConfigException("Missing field: " + path + "select")
            } else {
                select!!.validate(path)
            }
        }
        if (parallelLoad != null) {
            parallelLoad!!.validate(path)
        }
    }
}