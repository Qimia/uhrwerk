package io.qimia.uhrwerk

import org.scalatest.flatspec.AnyFlatSpec

class ScalaSimpleCollectionTests extends AnyFlatSpec {

  "Adding a new entry to the Map" should "work" in {
    val mp = Map("a" -> 1, "b" -> 2)
    val mp2 = mp + ("c" -> 3)
    assert(mp2("c") == 3)
  }

  "Adding two maps together" should "work" in {
    val mp1 = Map("a" -> 1, "b" -> 2)
    val mp2 = Map("b" -> 20, "c" -> 3, "d" -> 4)
    val mp3 = mp1 ++ mp2
    assert(mp3.size == mp1.size + (mp2.size - 1))
    assert(mp3("b") == 20)
  }

  "FlatMap on Options" should "work" in {
    val orig = Array(1, 2, 3, 4, 5)
    val evens = orig.flatMap(x => if (x % 2 == 0) Some(x) else None)
    assert(evens.length == 2)
    assert(evens === Array(2, 4))
  }

  "(Option) Pairs to Map" should "work" in {
    val pairs = Array((1, "a"), (2, "b"), (3, "c"))

    val mp = pairs
      .flatMap({ case (k, v) =>
        if (k % 2 == 0)
          None
        else
          Some(k -> v)
      })
      .toMap

    assert(mp.isInstanceOf[Map[Int, String]] == true)
    assert(mp.size == 2)
    assert(mp(1) == "a")
    assert(mp(3) == "c")
    assert(mp.get(2).isEmpty == true)
  }

}
