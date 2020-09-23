package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

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

  }

}