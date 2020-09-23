package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.PartitionTransformType;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransformBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

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

    logger.info(transform1);
    logger.info(transform2);
    logger.info(transform3);
    logger.info(transform4);

    assertEquals("identity", transform1.getType());
    assertEquals(null, transform1.getPartition());


    assertEquals("aggregate", transform2.getType());
    assertEquals(2, transform2.getPartition().getSize());

    assertEquals("window", transform3.getType());
    assertEquals(3, transform3.getPartition().getSize());

    assertEquals("temporal_aggregate", transform4.getType());
    assertEquals(4, transform4.getPartition().getSize());

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

    logger.info(transform1);
    logger.info(transform2);

    assertEquals("temporal_aggregate", transform1.getType());
    assertEquals(10, transform1.getPartition().getSize());

    assertEquals("temporal_aggregate", transform2.getType());
    assertEquals(5, transform2.getPartition().getSize());
  }


}