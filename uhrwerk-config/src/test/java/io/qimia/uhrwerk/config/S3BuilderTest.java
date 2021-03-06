package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3BuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new S3Builder();

    var s3 = builder
            .path("myPath")
            .secretId("secretID")
            .secretKey("secretKey")
            .build();
    logger.info(s3);

    assertEquals("myPath", s3.getPath());

  }

}