package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class DependencyBuilderTest {

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
            .type("temporal_aggregate")
            .partition()
            .unit("hours")
            .size(1)
            .done()
            .done()
            .build();
    System.out.println(dependency);
  }

  @Test
  void nestedBuilderTest1() {
    var builder = new DependencyBuilder();
    var partition = new PartitionBuilder<>().unit("hours").size(1).build();
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
    System.out.println(dependency);
  }

  @Test
  void nestedBuilderTest2() {
    var builder = new DependencyBuilder();
    var transform =
        new TransformBuilder().type("identity").partition().unit("hours").size(1).done();
    var dependency =
        builder
            .area("area")
            .vertical("vertical")
            .table("table")
            .format("json")
            .version("1.0")
            .transform(transform)
            .build();
    System.out.println(dependency);
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

    System.out.println(dependency1);
      System.out.println(dependency2);
  }
}