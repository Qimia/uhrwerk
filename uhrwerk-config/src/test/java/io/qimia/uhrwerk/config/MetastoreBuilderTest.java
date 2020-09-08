package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class MetastoreBuilderTest {

  @Test
  void builderTest() {
    var builder = new MetastoreBuilder();

    var metastore = builder
            .jdbcUrl("myURL")
            .jdbcDriver("myDriver")
            .user("user")
            .pass("pass")
            .build();

    System.out.println(metastore);

  }

}