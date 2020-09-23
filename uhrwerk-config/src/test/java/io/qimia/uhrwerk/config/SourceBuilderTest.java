package io.qimia.uhrwerk.config;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

class SourceBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  void buildTest() {
    var source =
        new SourceBuilder()
            .connectionName("connection")
            .path("table")
            .format("jdbc")
            .version("1.0")
            .autoloading(false)
            .partition()
            .unit("hours")
            .size(1)
            .done()
            .parallelLoad()
            .query("Select * from TableA")
            .column("id")
            .num(8)
            .done()
            .select()
            .query("SELECT * FROM TableA")
            .column("created_ts")
            .done()
            .build();
    logger.info(source);
  }

  @Test
  void nestedBuildTest1() {

    var partition = new PartitionBuilder<>().unit("hours").size(1).build();

    var parallelLoad =
        new ParallelLoadBuilder().query("Select * from TableA").column("id").num(8).build();

    var select = new SelectBuilder().query("SELECT * FROM TableA").column("created_ts").build();

    var source =
        new SourceBuilder()
            .connectionName("connection")
            .path("table")
            .format("jdbc")
            .version("1.0")
            .partition(partition)
            .parallelLoad(parallelLoad)
            .select(select)
            .build();
    logger.info(source);
  }

  @Test
  void nestedBuildTest2() {

    var partition = new PartitionBuilder<>().unit("hours").size(1);

    var parallelLoad = new ParallelLoadBuilder().query("Select * from TableA").column("id").num(8);

    var select = new SelectBuilder().query("SELECT * FROM TableA").column("created_ts");

    var source =
        new SourceBuilder()
            .connectionName("connection")
            .path("table")
            .format("jdbc")
            .version("1.0")
            .partition(partition)
            .parallelLoad(parallelLoad)
            .select(select)
            .build();
    logger.info(source);
  }
}
