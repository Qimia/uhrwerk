package io.qimia.uhrwerk.repo

import io.qimia.uhrwerk.common.metastore.model.BaseModel
import org.slf4j.LoggerFactory
import java.sql.*

abstract class BaseRepo<E : BaseModel> {

    @Throws(SQLException::class)
    protected fun insert(
        entity: E,
        sql: String,
        setParams: (PreparedStatement) -> Unit
    ): E? {
        val connection = HikariCPDataSource.connection
        connection.use {
            val insert = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
            setParams(insert)
            insert.use {
                insert.executeUpdate()
                val keys = insert.generatedKeys
                keys.use {
                    keys.next()
                    val id = keys.getLong(1)
                    entity.id(id)
                    return entity
                }
            }
        }
    }

    @Throws(SQLException::class)
    protected fun update(
        query: String,
        setParam: (PreparedStatement) -> Unit
    ): Int? {
        val connection = HikariCPDataSource.connection
        connection.use {
            val delete = connection.prepareStatement(query)
            setParam(delete)
            delete.use {
                return delete.executeUpdate()
            }
        }
    }

    @Throws(SQLException::class)
    protected fun find(
        query: String,
        setParam: (PreparedStatement) -> Unit,
        map: (res: ResultSet) -> E
    ): E? {
        val connection = HikariCPDataSource.connection
        connection.use {
            val select = connection.prepareStatement(query)
            setParam(select)
            select.use {
                val res = select.executeQuery()
                res.use {
                    if (res.next()) {
                        return map(res)
                    }
                    return null
                }
            }
        }
    }

    @Throws(SQLException::class)
    protected fun findAll(
        query: String,
        setParam: (PreparedStatement) -> Unit,
        map: (res: ResultSet) -> E
    ): List<E> {
        val connection = HikariCPDataSource.connection
        connection.use {
            LOGGER.info(query)
            val select = connection.prepareStatement(query)
            setParam(select)
            select.use {
                val res = select.executeQuery()
                res.use {
                    val entities = mutableListOf<E>()
                    while (res.next()) entities.add(map(res))
                    return entities
                }
            }
        }
    }

    @Throws(SQLException::class)
    protected fun getByHashKey(
        query: String,
        setParam: (PreparedStatement) -> Unit,
        map: (res: ResultSet) -> E
    ): E? {
        val entities = findAll(query, setParam, map)
        if (entities.isNotEmpty())
            return entities.first()
        return null
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(BaseRepo::class.java)

        fun columnsToString(tableCols: List<String>): String =
            tableCols.joinToString(separator = ",\n")

        fun insertSql(tableName: String, tableCols: List<String>): String {
            val cols = tableCols.subList(1, tableCols.size - 1)
            val colsStr = columnsToString(cols)
            val qMarks = List(cols.size) { "?" }.joinToString()

            return "INSERT INTO $tableName($colsStr) \n" +
                    "VALUES ($qMarks)"

        }
    }
}