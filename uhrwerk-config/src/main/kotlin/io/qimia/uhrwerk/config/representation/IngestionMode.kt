package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty

enum class IngestionMode {
    @JsonProperty("interval")
    INTERVAL,
    @JsonProperty("delta")
    DELTA,
    @JsonProperty("all")
    ALL

}