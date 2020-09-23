package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new FileBuilder();

    var file = builder
            .path("myPath")
            .build();
    logger.info(file);

    assertEquals("myPath", file.getPath());
  }

}