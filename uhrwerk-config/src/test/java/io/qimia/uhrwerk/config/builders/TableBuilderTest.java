package io.qimia.uhrwerk.config.builders;


import io.qimia.uhrwerk.common.metastore.model.PartitionUnit;
import io.qimia.uhrwerk.common.metastore.model.TableModel;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableBuilderTest {
  private final Logger logger = LoggerFactory.getLogger(TableBuilderTest.class);

  @Test
  public void tableBuilderTest() {
    var builder = new TableBuilder();

    TableModel table = builder
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
              .format("csv")
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
              .format("jdbc")
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
              .format("jdbc")
            .done()
            .target()
              .connectionName("TargetConnection2")
              .format("jdbc")
            .done()
            .dependency()
              .area("DepArea1")
              .version("1.1")
              .vertical("DepVertical1")
              .table("DepTableTable1")
              .format("jdbc")
              .done()
            .dependency()
              .area("DepArea2")
              .version("1.2")
              .vertical("DepVertical2")
              .table("DepTableTable2")
              .format("jdbc")
            .done()
            .dependency()
              .area("DepArea3")
              .version("1.3")
              .vertical("DepVertical3")
              .table("DepTableTable3")
              .format("jdbc")
            .done()
            .dependency()
            .area("DepArea4")
            .version("1.4")
            .vertical("DepVertical4")
            .table("DepTableTable4")
            .format("jdbc")
            .done()
            .build();
    logger.info(table.toString());

    assertEquals("TableArea", table.getArea());
    assertEquals("TableArea.TableVertical.TableTable.TableVersion", table.getClassName());
    assertEquals(PartitionUnit.HOURS, table.getPartitionUnit());



  }

  @Test
  void nestedBuildTest1() {

    var partition =
            new PartitionBuilder<>()
                    .unit("days")
                    .size(10)
                    .build();

    var dependency =
            new DependencyBuilder()
                    .area("area")
                    .vertical("vertical")
                    .table("table")
                    .format("json")
                    .version("1.0")
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
                    .format("jdbc")
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
                .className("my.class.name")
                .parallelism(2)
                .maxBulkSize(2)
                .partition(partition)
                .source(source)
                .target(target)
                .dependency(dependency)
            .build();
    logger.info(table.toString());

    assertEquals("TableArea", table.getArea());
    assertEquals("my.class.name", table.getClassName());
    assertEquals("connection", table.getSources()[0].getConnection().getName());
    assertEquals("area", table.getDependencies()[0].getArea());
    assertEquals("conName", table.getTargets()[0].getConnection().getName());
    assertEquals(PartitionUnit.DAYS, table.getPartitionUnit());
  }

}
