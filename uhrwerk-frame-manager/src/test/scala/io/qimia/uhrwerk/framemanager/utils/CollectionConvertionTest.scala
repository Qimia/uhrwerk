package io.qimia.uhrwerk.framemanager.utils

import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.SortedSet

class CollectionConvertionTest extends AnyFlatSpec {
  "Scala List to SortedSet" should "work" in {

    val list = List("d", "e", "a", "b", "f", "g", "h", "c", "a", "h", "d")
    val set = list.to[SortedSet]
    assert(set === SortedSet("a", "b", "c", "d", "e", "f", "g", "h"))
  }

  "Scala merge list of maps" should "work" in {
    val list = List(
      Map("a" -> 1, "b" -> "2"),
      Map("a" -> 3, "b" -> "4"),
      Map("a" -> 5, "b" -> "6")
    )
    val merged = list.flatten.groupBy(_._1).mapValues(_.map(_._2))
    println(merged)
    assert(merged === Map("a" -> List(1, 3, 5), "b" -> List("2", "4", "6")))
  }
}
