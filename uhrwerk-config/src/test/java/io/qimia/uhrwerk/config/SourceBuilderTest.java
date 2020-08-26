package io.qimia.uhrwerk.config;

import org.junit.jupiter.api.Test;

class SourceBuilderTest {

  @Test
  void buildTest() {
    var source =
        new SourceBuilder()
            .connectionName("connection")
            .path("table")
            .format("JDBC")
            .version("1.0")
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
    System.out.println(source);
  }
}
