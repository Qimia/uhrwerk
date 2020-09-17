package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class TransformBuilderTest {

  @Test
  void builderTest() {
    var builder = new TransformBuilder();

    var transform1 = builder
            .type("identity")
            .build();

    var transform2 = builder
            .type("aggregate")
            .partition()
            .size(2)
            .done()
            .build();

    var transform3 = builder
            .type("window")
            .partition()
            .size(3)
            .done()
            .build();

    var transform4 = builder
            .type("temporal_aggregate")
            .partition()
            .size(4)
            .unit("hours")
            .done()
            .build();

    System.out.println(transform1);
    System.out.println(transform2);
    System.out.println(transform3);
    System.out.println(transform4);
  }

  @Test
  void nestedBuilderTest() {
    var builder = new TransformBuilder();
    var partitionBuilder = new PartitionBuilder<>().unit("hours").size(10);
    var partition = new PartitionBuilder<>().unit("days").size(5).build();

    var transform1 = builder
            .type("temporal_aggregate")
            .partition(partitionBuilder)
            .build();

    var transform2 = builder
            .type("temporal_aggregate")
            .partition(partition)
            .build();

    System.out.println(transform1);
    System.out.println(transform2);
  }


}