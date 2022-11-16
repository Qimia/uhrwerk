package io.qimia.uhrwerk.common.metastore.builders

import java.time.LocalDateTime

abstract class StateModelBuilder<B : StateModelBuilder<B>?> {
    var deactivatedTs: LocalDateTime? = null
        protected set

    fun deactivatedTs(deactivatedTs: LocalDateTime?): B {
        this.deactivatedTs = deactivatedTs
        return getThis()
    }

    abstract fun getThis(): B

}