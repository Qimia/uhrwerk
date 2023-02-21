package io.qimia.uhrwerk.framemanager.utils

import org.scalatest.flatspec.AnyFlatSpec

class SparkFrameManagerUtilsTest extends AnyFlatSpec {
  "SparkFrameManagerUtils.getFullLocation" should "work" in {
    val location =
      SparkFrameManagerUtils.getFullLocation("s3a://bucket", "path")
    assert(location.head == "s3a://bucket/path")
    assert(location.tail.isEmpty)

    val location2 = SparkFrameManagerUtils.getFullLocation(
      "s3a://bucket",
      "path",
      List("col1=val1/col2=val2")
    )
    assert(location2.head == "s3a://bucket/path/col1=val1/col2=val2")
    assert(location2.tail.isEmpty)
  }
}
