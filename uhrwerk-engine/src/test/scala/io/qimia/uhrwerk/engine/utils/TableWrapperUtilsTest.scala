package io.qimia.uhrwerk.engine.utils

import org.scalatest.funsuite.AnyFunSuiteLike

class TableWrapperUtilsTest extends AnyFunSuiteLike {

  test("testNormalizeViewName") {
    assert(TableWrapperUtils.normalizeViewName("test.test") == "test___test")
  }

}
