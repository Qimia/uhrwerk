package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class SelectBuilderTest {

  @Test
  void builderTest() {
    var builder = new SelectBuilder();

    var select = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .build();
    System.out.println(select);
  }

}