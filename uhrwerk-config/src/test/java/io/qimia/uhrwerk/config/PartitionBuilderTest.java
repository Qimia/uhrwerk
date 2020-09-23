package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class PartitionBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new PartitionBuilder<>();

    var partition =
        builder
            .unit("hour")
            .size(10)
            .build();
    logger.info(partition);
  }

}