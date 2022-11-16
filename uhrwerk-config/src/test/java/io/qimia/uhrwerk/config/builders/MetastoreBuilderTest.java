package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetastoreBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new MetastoreBuilder();

    var metastore = builder
            .jdbcUrl("myURL")
            .jdbcDriver("myDriver")
            .user("user")
            .pass("pass")
            .build();

    logger.info(metastore.toString());


    assertEquals("pass", metastore.getPass());
  }

}