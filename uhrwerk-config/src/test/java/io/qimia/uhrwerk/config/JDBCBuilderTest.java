package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JDBCBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  void builderTest() {
    var builder = new JDBCBuilder();

    var jdbc = builder
            .jdbcDriver("myDriver")
            .jdbcUrl("myURL")
            .user("user")
            .pass("pass")
            .build();
    logger.info(jdbc.toString());

    assertEquals("myDriver", jdbc.getJdbc_driver());
    assertEquals( "myURL", jdbc.getJdbc_url());
    assertEquals("user", jdbc.getUser());
    assertEquals("pass", jdbc.getPass());

  }

}