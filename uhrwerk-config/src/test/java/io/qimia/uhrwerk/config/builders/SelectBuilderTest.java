package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SelectBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new SelectBuilder();

    var select = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .build();
    logger.info(select.toString());

    assertEquals("id", select.getColumn());
  }

}