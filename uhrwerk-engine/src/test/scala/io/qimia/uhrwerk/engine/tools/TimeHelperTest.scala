package io.qimia.uhrwerk.engine.tools

import java.time.{Duration, LocalDateTime}

import org.scalatest.flatspec.AnyFlatSpec

class TimeHelperTest extends AnyFlatSpec {

  "given a list of LocalDateTime with some gaps and a max-group, it" should "be sortable in sequential groups" in {
    val duration = Duration.ofMinutes(20)
    val times = List(
      LocalDateTime.of(2010, 4, 5, 10, 0),
      LocalDateTime.of(2010, 4, 5, 10, 20),
      LocalDateTime.of(2010, 4, 5, 10, 40),
      LocalDateTime.of(2010, 4, 5, 11, 0),
      LocalDateTime.of(2010, 4, 5, 12, 0),
      LocalDateTime.of(2010, 4, 5, 12, 40),
      LocalDateTime.of(2010, 4, 5, 13, 0),
    )
    val outRes = TimeHelper.groupSequentialIncreasing(times, duration, 3)
    assert(outRes === Array(3, 1, 1, 2))
  }

}
