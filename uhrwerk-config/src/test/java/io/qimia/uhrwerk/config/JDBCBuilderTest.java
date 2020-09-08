package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class JDBCBuilderTest {

  @Test
  void builderTest() {
    var builder = new JDBCBuilder();

    var jdbc = builder
            .jdbcDriver("myDriver")
            .jdbcUrl("myURL")
            .user("user")
            .pass("pass")
            .build();
    System.out.println(jdbc);
  }

}