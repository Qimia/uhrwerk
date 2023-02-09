package io.qimia.uhrwerk.tools

import io.qimia.uhrwerk.common.metastore.model.Partition
import io.qimia.uhrwerk.config.representation.Reference
import io.qimia.uhrwerk.dao.PartitionDAO
import io.qimia.uhrwerk.dao.TableDAO
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge

object UhrwerkDagBuilder {

    val tables = TableDAO()
    val partitions = PartitionDAO()

    fun process(ref: Reference): List<TableInfo> {
        val dag =
            DefaultDirectedGraph<TableInfo, DefaultEdge>(DefaultEdge::class.java)
        val target = table(ref)
        buildGraph(target, dag)

        val revDag =
            DefaultDirectedGraph<TableInfo, DefaultEdge>(DefaultEdge::class.java)
        traverse(target, dag, revDag)
        return revDag.vertexSet().toList()
    }

    fun dag(ref: Reference): List<TableInfo> {
        val dag =
            DefaultDirectedGraph<TableInfo, DefaultEdge>(DefaultEdge::class.java)
        val target = table(ref)
        buildGraph(target, dag)
        return dag.vertexSet().toList()
    }

    fun table(ref: Reference): TableInfo {

        val res = TableInfo(ref)
        val table = tables[ref.area!!, ref.vertical!!, ref.table!!, ref.version!!]
        if (table != null) {
            res.exists = true
            val partition: Partition? = if (!table.targets.isNullOrEmpty()) {
                partitions.getLatestPartition(table.targets!![0].id!!)
            } else null
            res.processed = partition != null
        }
        return res
    }

    fun buildGraph(
        info: TableInfo,
        dag: DefaultDirectedGraph<TableInfo, DefaultEdge>,
        depth: Int = 0
    ) {
        info.depth = depth
        dag.addVertex(info)
        val ref = info.ref
        if (info.exists) {
            val table = tables[ref.area!!, ref.vertical!!, ref.table!!, ref.version!!]
            if (!table!!.dependencies.isNullOrEmpty()) {
                table.dependencies!!.forEach {
                    run {
                        val depRef = Reference(it.area, it.vertical, it.tableName, it.version)
                        val depInfo = table(depRef)
                        buildGraph(
                            depInfo,
                            dag, depth + 1
                        )
                        dag.addEdge(depInfo, info)
                    }
                }
            }
        }
    }

    fun traverse(
        node: TableInfo,
        dag: DefaultDirectedGraph<TableInfo, DefaultEdge>,
        revDag: DefaultDirectedGraph<TableInfo, DefaultEdge>
    ): TableInfo {
        if (!node.exists) {
            return node.copy()
        }
        val prevNodes = dag.incomingEdgesOf(node).map { dag.getEdgeSource(it) }
        return if (prevNodes.isNullOrEmpty()) {
            val cp = node.copy()
            revDag.addVertex(cp)
            cp
        } else {
            val tpNodes = prevNodes.map { traverse(it, dag, revDag) }
            val process = node.processed && tpNodes.map { it.processed }.reduce { l, r -> l && r }
            val nwNode = node.copy(processed = process)
            revDag.addVertex(nwNode)
            tpNodes.forEach { revDag.addEdge(it, nwNode) }
            nwNode
        }

    }


}