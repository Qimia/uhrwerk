package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class TargetBuilderTest {

  @Test
  void builderTest() {
    var builder = new TargetBuilder();

    var target =
        builder
            .connectionName("conName")
            .format("csv")
            .build();
    System.out.println(target);
  }

}