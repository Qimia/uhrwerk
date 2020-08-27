package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class FileBuilderTest {

  @Test
  void builderTest() {
    var builder = new FileBuilder();

    var file = builder
            .path("myPath")
            .build();
    System.out.println(file);
  }

}