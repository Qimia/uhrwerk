package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Table;
import org.junit.jupiter.api.Test;

class TableBuilderTest {

  @Test
  public void tableBuilderTest() {
    var builder = new TableBuilder();

    Table table = builder
            .area("TableArea")
            .vertical("TableVertical")
            .table("TableTable")
            .version("TableVersion")
            .parallelism(2)
            .maxBulkSize(2)
            .partition()
              .unit("hours")
              .size(4)
            .done()
            .source()
              .connectionName("SourceConnection1")
              .path("SourcePath1")
              .format("SourceFormat1")
              .version("1.0")
              .partition()
                .unit("days")
                .size(10)
              .done()
              .parallelLoad()
                .query("SELECT * FROM BLA1")
                .column("Column1")
                .num(10)
              .done()
              .select()
                .query("config/table_test_2_select_query.sql")
                .column("created_at")
              .done()
            .done()
            .source()
              .connectionName("SourceConnection2")
              .path("SourcePath2")
              .format("SourceFormat2")
              .version("1.0")
              .partition()
                .unit("hours")
                .size(24)
              .done()
              //.parallel_load()
              //  .query("SELECT * FROM BLA2")
              //  .column("Column2")
              //  .num(5)
              .select()
                .query("SELECT * FROM BLA3")
                .column("Column3")
              .done()
            .done()
            .target()
              .connectionName("TargetConnection1")
              .format("TargetFormat1")
            .done()
            .target()
              .connectionName("TargetConnection2")
              .format("TargetFormat2")
            .done()
            .dependency()
              .area("DepArea1")
              .version("1.1")
              .vertical("DepVertical1")
              .table("DepTableTable1")
              .format("DepFormat1")
              .transform()
                .type("identity")
              .done()
            .done()
            .dependency()
              .area("DepArea2")
              .version("1.2")
              .vertical("DepVertical2")
              .table("DepTableTable2")
              .format("DepFormat2")
              .transform()
                .type("window")
                .partition()
                  .size(5)
                .done()
              .done()
            .done()
            .dependency()
              .area("DepArea3")
              .version("1.3")
              .vertical("DepVertical3")
              .table("DepTableTable3")
              .format("DepFormat3")
              .transform()
                .type("aggregate")
                .partition()
                  .size(2)
                .done()
              .done()
            .done()
            .dependency()
              .area("DepArea4")
              .version("1.4")
              .vertical("DepVertical4")
              .table("DepTableTable4")
              .format("DepFormat4")
              .transform()
                .type("temporal_aggregate")
                .partition()
                  .size(4)
                  .unit("hours")
                .done()
              .done()
            .done()
            .build();
    System.out.println(table);

  }

  @Test
  void nestedBuildTest1() {

    var partition =
            new PartitionBuilder<>()
                    .unit("day")
                    .size(10)
                    .build();

    var dependency =
            new DependencyBuilder()
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

    var target =
            new TargetBuilder()
                    .connectionName("conName")
                    .format("csv")
                    .build();

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

    var table =
        new TableBuilder()
                .area("TableArea")
                .vertical("TableVertical")
                .table("TableTable")
                .version("TableVersion")
                .parallelism(2)
                .maxBulkSize(2)
                .partition(partition)
                .source(source)
                .target(target)
                .dependency(dependency)
            .build();
    System.out.println(table);
  }

}
