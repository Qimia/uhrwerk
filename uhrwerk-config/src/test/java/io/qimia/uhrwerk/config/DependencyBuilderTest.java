package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class DependencyBuilderTest {

  @Test
  void builderTest() {
    var builder = new DependencyBuilder();

    var partitionBuilder = new PartitionBuilder();

    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .transform()
            .type("identity")
            .partition()
            .unit("hours")
            .size(1)
            .done()
            .done()
            .build();
    System.out.println(dependency);
  }
}
