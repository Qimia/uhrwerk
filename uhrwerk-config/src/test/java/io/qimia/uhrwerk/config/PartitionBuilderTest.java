package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new PartitionBuilder<>();

    var partition =
        builder
            .unit("hour")
            .size(10)
            .build();
    logger.info(partition.toString());

    assertEquals(10, partition.getSize());
    assertEquals("hour", partition.getUnit());
  }

}