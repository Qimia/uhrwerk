package io.qimia.uhrwerk.repo

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.SQLException

object HikariCPDataSource {
    private var config: HikariConfig? = null

    @get:Synchronized
    internal var datasource: HikariDataSource? = null
        internal get() {
            if (field == null || field!!.isClosed) field = HikariDataSource(config)
            return field
        }
        private set

    @Synchronized
    @JvmStatic
    fun initConfig(
        jdbcUrl: String?, userName: String?, password: String?
    ): HikariConfig? {
        if (config != null) return config
        config = HikariConfig()
        config!!.jdbcUrl = jdbcUrl
        config!!.username = userName
        config!!.password = password
        config!!.addDataSourceProperty("cachePrepStmts", "true")
        config!!.addDataSourceProperty("prepStmtCacheSize", "250")
        config!!.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
        return config
    }

    @Synchronized
    fun close() {
        config = null
        datasource!!.close()
        datasource = null
    }

    @get:Throws(SQLException::class)
    val connection: Connection
        get() = datasource!!.connection
}