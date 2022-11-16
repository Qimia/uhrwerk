package io.qimia.uhrwerk.dao

import io.qimia.uhrwerk.common.metastore.model.DependencyModel
import net.openhft.hashing.LongHashFunction
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class HashIdTest {
    private val logger = LoggerFactory.getLogger(this.javaClass)
    @Test
    fun test() {
        val dep = DependencyModel()
        dep.area = "test-area"
        dep.vertical = "test-vertical"
        dep.tableName = "test-table"
        dep.format = "json"
        dep.version = "version"
        var res = StringBuilder()
                .append(dep.area)
                .append(dep.vertical)
                .append(dep.tableName)
                .append(dep.version)
        logger.info("without format: $res")
        val tableId = LongHashFunction.xx().hashChars(res)
        logger.info("tableId: $tableId")
        res = res.append(dep.format)
        logger.info("with format: $res")
        val depId = LongHashFunction.xx().hashChars(res)
        logger.info("depId: $depId")
    }
}