package io.qimia.uhrwerk.config.representation

import com.fasterxml.jackson.annotation.JsonProperty

data class Redshift(
    override var name: String? = null,
    @JsonProperty("jdbc_url")
    var jdbcUrl: String? = null,
    var format: String? = "io.github.spark_redshift_community.spark.redshift",
    var user: String? = null,
    var password: String? = null,
    @JsonProperty("aws_iam_role")
    var awsIamRole: String? = null,
    @JsonProperty("temp_dir")
    var tempDir: String? = null
) : Connection() {
    override fun validate(path: String) {
    }
}