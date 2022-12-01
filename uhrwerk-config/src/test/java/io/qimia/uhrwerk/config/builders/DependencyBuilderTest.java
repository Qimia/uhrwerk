package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DependencyBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(DependencyBuilderTest.class);

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
            .build();
    logger.info(dependency.toString());

    assertEquals("area", dependency.getReference().getArea());
  }

  @Test
  void nestedBuilderTest1() {
    var builder = new DependencyBuilder();
    var partition = new PartitionBuilder<>().unit("hours").size(2).build();
    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .build();
    logger.info(dependency.toString());

    assertEquals("area", dependency.getReference().getArea());
  }

  @Test
  void nestedBuilderTest2() {
    var builder = new DependencyBuilder();
    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .build();
    logger.info(dependency.toString());

    assertEquals("area", dependency.getReference().getArea());
  }


  @Test
  void nestedBuilderTest3() {
    var builder = new DependencyBuilder();
    var dependency1 =
            builder
                    .area("area")
                    .vertical("vertical")
                    .table("table")
                    .format("json")
                    .version("1.0")
                    .build();

    var dependency2 = builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .build();

    logger.info(dependency1.toString());
    logger.info(dependency2.toString());

    assertEquals("area", dependency1.getReference().getArea());

    assertEquals("area", dependency1.getReference().getArea());
  }
}