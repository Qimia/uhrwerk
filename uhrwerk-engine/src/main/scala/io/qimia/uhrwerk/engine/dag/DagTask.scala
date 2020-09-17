package io.qimia.uhrwerk.engine.dag

import java.time.LocalDateTime

import io.qimia.uhrwerk.engine.TableWrapper

case class DagTask(
    table: TableWrapper,
    partitions: Seq[LocalDateTime],
    dagDept: Int = 0
)
