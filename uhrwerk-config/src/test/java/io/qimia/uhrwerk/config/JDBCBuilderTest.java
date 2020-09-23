package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class JDBCBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new JDBCBuilder();

    var jdbc = builder
            .jdbcDriver("myDriver")
            .jdbcUrl("myURL")
            .user("user")
            .pass("pass")
            .build();
    logger.info(jdbc);
  }

}