package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class ParallelLoadBuilderTest {

  @Test
  void builderTest() {
    var builder = new ParallelLoadBuilder();

    var parallelLoad = builder
            .column("id")
            .query("SELECT * from TABLE1")
            .num(5)
            .build();
    System.out.println(parallelLoad);
  }

}