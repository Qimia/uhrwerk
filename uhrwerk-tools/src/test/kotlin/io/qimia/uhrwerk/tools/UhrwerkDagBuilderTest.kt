package io.qimia.uhrwerk.tools

import com.fasterxml.jackson.databind.ObjectMapper
import io.qimia.uhrwerk.config.representation.Reference
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.junit.jupiter.api.Test


class UhrwerkDagBuilderTest {

    @Test
    fun test() {

        val dag =
            DefaultDirectedGraph<TableInfo, DefaultEdge>(DefaultEdge::class.java)

        val rootRef = Reference("area", "vertical", "root", "1.0")
        val root = TableInfo(rootRef,0)
        val dep1Ref = Reference("area", "vertical", "dep1", "1.0")
        val dep1 = TableInfo(dep1Ref,1)
        val dep2Ref = Reference("area", "vertical", "dep2", "1.0")
        val dep2= TableInfo(dep2Ref,1)

        dag.addVertex(root)
        dag.addVertex(dep1)
        dag.addVertex(dep2)
        dag.addEdge(dep1,root)
        dag.addEdge(dep2,root)


        val dep1Dep1Ref = Reference("area", "vertical", "dep1Dep1", "1.0")
        val dep1Dep1 =TableInfo(dep1Dep1Ref,2)

        val dep1Dep2Ref = Reference("area", "vertical", "dep1Dep2", "1.0")
        val dep1Dep2 =TableInfo(dep1Dep2Ref,2)

        val dep2Dep1Ref = Reference("area", "vertical", "dep2Dep1", "1.0")
        val dep2Dep1 =TableInfo(dep2Dep1Ref,2)

        dag.addVertex(dep1Dep1)
        dag.addVertex(dep1Dep2)
        dag.addVertex(dep2Dep1)
        dag.addEdge(dep1Dep1,dep1)
        dag.addEdge(dep1Dep2,dep1)
        dag.addEdge(dep2Dep1,dep2)

        println(ObjectMapper().writeValueAsString(root))

        println(root.ref.toString())

        println(ObjectMapper().writeValueAsString(dag))
    }
}