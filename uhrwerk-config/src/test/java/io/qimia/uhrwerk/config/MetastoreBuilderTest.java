package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MetastoreBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new MetastoreBuilder();

    var metastore = builder
            .jdbcUrl("myURL")
            .jdbcDriver("myDriver")
            .user("user")
            .pass("pass")
            .build();

    logger.info(metastore);


    assertEquals("pass", metastore.getPass());
  }

}