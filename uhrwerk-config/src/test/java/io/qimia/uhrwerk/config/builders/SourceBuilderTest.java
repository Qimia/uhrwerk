package io.qimia.uhrwerk.config.builders;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SourceBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

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
    logger.info(source.toString());

    assertEquals(8, source.getParallelLoad().getNum());
    assertEquals("connection", source.getConnectionName());
    assertEquals("created_ts", source.getSelect().getColumn());
  }

  @Test
  void nestedBuildTest1() {

    var partition = new PartitionBuilder<>().unit("hours").size(1).build();

    var parallelLoad =
        new ParallelLoadBuilder().query("Select * from TableA").column("id").num(6).build();

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
    logger.info(source.toString());

    assertEquals(6, source.getParallelLoad().getNum());
    assertEquals("connection", source.getConnectionName());
    assertEquals("created_ts", source.getSelect().getColumn());

  }

  @Test
  void nestedBuildTest2() {

    var partition = new PartitionBuilder<>().unit("hours").size(1);

    var parallelLoad = new ParallelLoadBuilder().query("Select * from TableA").column("id").num(5);

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
    logger.info(source.toString());

    assertEquals(5, source.getParallelLoad().getNum());
    assertEquals("connection", source.getConnectionName());
    assertEquals("created_ts", source.getSelect().getColumn());

  }
}
