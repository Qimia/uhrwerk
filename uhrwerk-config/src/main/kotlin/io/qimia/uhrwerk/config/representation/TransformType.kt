package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty

enum class TransformType {
    @JsonProperty("identity")
    IDENTITY,

    @JsonProperty("aggregate")
    AGGREGATE,

    @JsonProperty("window")
    WINDOW,

    @JsonProperty("none")
    NONE
}