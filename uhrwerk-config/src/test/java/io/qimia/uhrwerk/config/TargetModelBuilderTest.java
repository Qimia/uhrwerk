package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TargetModelBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new TargetBuilder();

    var target =
        builder
            .connectionName("conName")
            .format("csv")
            .build();
    logger.info(target.toString());

    assertEquals("conName", target.getConnection_name());
    assertEquals("csv", target.getFormat());

  }


}