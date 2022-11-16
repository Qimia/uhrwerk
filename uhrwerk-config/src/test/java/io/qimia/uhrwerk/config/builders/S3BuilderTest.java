package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3BuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderCredsTest() {
    var builder = new S3Builder();

    var s3 = builder
            .path("myPath")
            .secretId("secretID")
            .secretKey("secretKey")
            .build();
    logger.info(s3.toString());

    assertEquals("myPath", s3.getPath());

  }

  @Test
  void builderNoCredsTest() {
    var builder = new S3Builder();

    var s3 = builder
            .path("myPath")
            .build();
    logger.info(s3.toString());

    assertEquals("myPath", s3.getPath());

  }

}