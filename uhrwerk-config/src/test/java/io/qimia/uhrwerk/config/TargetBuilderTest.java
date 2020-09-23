package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class TargetBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new TargetBuilder();

    var target =
        builder
            .connectionName("conName")
            .format("csv")
            .build();
    logger.info(target);
  }

}