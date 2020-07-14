package io.qimia.uhrwerk.ManagedIO

import java.time.{Duration, LocalDateTime}

import org.scalatest.flatspec.AnyFlatSpec

class SparkFrameManagerTest extends AnyFlatSpec {

  "When converting a LocalDateTime to a postfix it" should "be easily convertable" in {
    val dateTime = LocalDateTime.of(2020, 1, 1, 8, 45)
    val duration = Duration.ofMinutes(15)
    val outStr = SparkFrameManager.dateTimeToPostFix(dateTime, duration)
    val corrStr = s"/year=2020/month=2020-1/day=2020-1-1/hour=8/batch=3"
    assert(outStr === corrStr)
  }

}
