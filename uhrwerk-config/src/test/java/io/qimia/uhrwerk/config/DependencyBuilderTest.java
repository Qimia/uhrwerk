package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DependencyBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void builderTest() {
    var builder = new DependencyBuilder();

    var dependency =
        builder
                .area("area")
                .vertical("vertical")
                .table("table")
                .format("json")
                .version("1.0")
                .transform()
                .type("aggregate")
            .partition()
            .unit("hours")
            .size(1)
            .done()
            .done()
            .build();
    logger.info(dependency);

    assertEquals("area", dependency.getArea());
    assertEquals(1, dependency.getTransform().getPartition().getSize());
  }

  @Test
  void nestedBuilderTest1() {
    var builder = new DependencyBuilder();
    var partition = new PartitionBuilder<>().unit("hours").size(2).build();
    var transform = new TransformBuilder().type("identity").partition(partition).build();
    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .transform(transform)
            .build();
    logger.info(dependency);

    assertEquals("area", dependency.getArea());
    assertEquals(2, dependency.getTransform().getPartition().getSize());
  }

  @Test
  void nestedBuilderTest2() {
    var builder = new DependencyBuilder();
    var transform =
        new TransformBuilder().type("identity").partition().unit("hours").size(3).done();
    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .transform(transform)
            .build();
    logger.info(dependency);

    assertEquals("area", dependency.getArea());
    assertEquals(3, dependency.getTransform().getPartition().getSize());
  }


  @Test
  void nestedBuilderTest3() {
    var builder = new DependencyBuilder();
    var transformBuilder =
            new TransformBuilder().type("identity");
    var transform =
            new TransformBuilder().type("identity").build();
    var dependency1 =
            builder
                    .area("area")
                    .vertical("vertical")
                    .table("table")
                    .format("json")
                    .version("1.0")
                    .transform(transformBuilder)
                    .build();

    var dependency2 = builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .transform(transform)
            .build();

    logger.info(dependency1);
    logger.info(dependency2);

    assertEquals("area", dependency1.getArea());
    assertEquals("identity", dependency1.getTransform().getType());

    assertEquals("area", dependency1.getArea());
    assertEquals("identity", dependency1.getTransform().getType());
  }
}