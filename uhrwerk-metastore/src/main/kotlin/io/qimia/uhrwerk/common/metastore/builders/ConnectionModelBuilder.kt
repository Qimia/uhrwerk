package io.qimia.uhrwerk.common.metastore.builders

import io.qimia.uhrwerk.common.metastore.model.ConnectionModel
import io.qimia.uhrwerk.common.metastore.model.ConnectionType

class ConnectionModelBuilder : StateModelBuilder<ConnectionModelBuilder>() {
    var id: Long? = null
    var name: String? = null
    var type: ConnectionType? = null
    var path: String? = null
    var jdbcUrl: String? = null
    var jdbcDriver: String? = null
    var jdbcUser: String? = null
    var jdbcPass: String? = null
    var awsAccessKeyID: String? = null
    var awsSecretAccessKey: String? = null

    fun id(id: Long?): ConnectionModelBuilder {
        this.id = id
        return this
    }

    fun name(name: String): ConnectionModelBuilder {
        this.name = name
        return this
    }

    fun type(type: ConnectionType?): ConnectionModelBuilder {
        this.type = type
        return this
    }

    fun path(path: String?): ConnectionModelBuilder {
        this.path = path
        return this
    }

    fun jdbcUrl(jdbcUrl: String?): ConnectionModelBuilder {
        this.jdbcUrl = jdbcUrl
        return this
    }

    fun jdbcDriver(jdbcDriver: String?): ConnectionModelBuilder {
        this.jdbcDriver = jdbcDriver
        return this
    }

    fun jdbcUser(jdbcUser: String?): ConnectionModelBuilder {
        this.jdbcUser = jdbcUser
        return this
    }

    fun jdbcPass(jdbcPass: String?): ConnectionModelBuilder {
        this.jdbcPass = jdbcPass
        return this
    }

    fun awsAccessKeyID(awsAccessKeyID: String?): ConnectionModelBuilder {
        this.awsAccessKeyID = awsAccessKeyID
        return this
    }

    fun awsSecretAccessKey(awsSecretAccessKey: String?): ConnectionModelBuilder {
        this.awsSecretAccessKey = awsSecretAccessKey
        return this
    }

    fun build(): ConnectionModel {
        return ConnectionModel(id,
        name,
        type,
        path,
        jdbcUrl,
        jdbcDriver,
        jdbcUser,
        jdbcPass,
        awsAccessKeyID,
        awsSecretAccessKey)
    }

    override fun getThis() = this

}