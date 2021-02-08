package io.qimia.uhrwerk.engine.dag

import io.qimia.uhrwerk.engine.Environment.TableIdent

import java.time.LocalDateTime

/**
 * A dependency in the execution dag.
 * Plus it can be used as key in the hashtable.
 * @param ident
 * @param partitions
 */
case class DT2Key(ident: TableIdent, partition: LocalDateTime)
