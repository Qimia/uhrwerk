package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty

data class InputView(
    @JsonProperty("input_view")
    var inputView: String? = null,
    @JsonProperty("table_view")
    var tableView: String? = null
)
