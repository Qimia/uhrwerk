package io.qimia.uhrwerk

import org.scalatest.flatspec.AnyFlatSpec

class MainTester extends AnyFlatSpec {

  "Testing the tester" should "run without a hitch" in {
    val x = Uhrstart.someFunc()
    assert(x === "Hey!")
  }

}
