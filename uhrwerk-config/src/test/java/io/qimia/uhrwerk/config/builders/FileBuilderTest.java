package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new FileBuilder();

    var file = builder.path("myPath").build();
    file.setName("file-connection");
    logger.info(file.toString());

    assertEquals("myPath", file.getPath());
  }
}
