package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class PartitionBuilderTest {

  @Test
  void builderTest() {
    var builder = new PartitionBuilder<>();

    var partition =
        builder
            .unit("hour")
            .size(10)
            .build();
    System.out.println(partition);
  }

}