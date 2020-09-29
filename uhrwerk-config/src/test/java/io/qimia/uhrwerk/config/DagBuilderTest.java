package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Dag;
import io.qimia.uhrwerk.common.model.PartitionTransformType;
import io.qimia.uhrwerk.config.representation.Connection;
import io.qimia.uhrwerk.config.representation.Table;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DagBuilderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  public void dagBuilderTest() {

    Dag dag = (new DagBuilder())
            .connection()
            .name("S3")
            .s3()
            .path("S3Path")
            .secretId("ID")
            .secretKey("key")
            .done()
            .done()
            .connection()
            .name("JDBC")
            .jdbc()
            .jdbcUrl("url")
            .jdbcDriver("driver")
            .user("user")
            .pass("pass")
            .done()
            .done()
            .connection()
            .name("file")
            .file()
            .path("filePath")
            .done()
            .done()
            .table()
            .area("TableArea")
            .vertical("TableVertical")
            .table("TableTable")
            .version("TableVersion")
            .className("my.new.class.name")
            .parallelism(2)
            .maxBulkSize(2)
            .partition()
            .unit("days")
            .size(4)
            .done()
            .source()
            .connectionName("SourceConnection1")
            .path("SourcePath1")
            .format("jdbc")
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
            .format("csv")
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
            .format("avro")
            .done()
            .target()
            .connectionName("TargetConnection2")
            .format("text")
            .done()
            .dependency()
            .area("DepArea1")
            .version("1.1")
            .vertical("DepVertical1")
            .table("DepTableTable1")
            .format("json")
            .transform()
            .type("identity")
            .done()
            .done()
            .dependency()
            .area("DepArea2")
            .version("1.2")
            .vertical("DepVertical2")
            .table("DepTableTable2")
            .format("text")
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
            .format("libsvm")
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
            .format("parquet")
            .transform()
            .type("aggregate")
            .partition()
            .size(4)
            .unit("days")
            .done()
            .done()
            .done()
            .done()
            .build();

    logger.info(dag);

    assertEquals("TableTable", dag.getTables()[0].getName());
    assertEquals("TableArea.TableVertical.TableTable.TableVersion", dag.getTables()[0].getClassName());
    assertEquals(4, dag.getTables()[0].getPartitionSize());



  }

  @Test
  void nestedBuildTest1() {

    Table table1 = new TableBuilder()
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
            .format("jdbc")
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
            .transform()
            .type("identity")
            .done()
            .done()
            .dependency()
            .area("DepArea2")
            .version("1.2")
            .vertical("DepVertical2")
            .table("DepTableTable2")
            .format("jdbc")
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
            .format("jdbc")
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
            .format("jdbc")
            .transform()
            .type("aggregate")
            .partition()
            .size(4)
            .unit("hours")
            .done()
            .done()
            .done()
            .buildRepresentationTable();


    Table table2 = new Table();
    table2.setTargets(table1.getTargets());
    table2.setDependencies(table1.getDependencies());
    table2.setSources(table1.getSources());
    table2.setPartition(table1.getPartition());
    table2.setMax_bulk_size(table1.getMax_bulk_size());
    table2.setParallelism(table1.getParallelism());
    table2.setVersion(table1.getVersion());
    table2.setVertical(table1.getVertical());
    table2.setTable(table1.getTable());
    table2.setArea("somedifferent");
    table2.setClass_name("my.class.name");

    var connection1 = new ConnectionBuilder()
            .name("s3")
            .s3()
            .path("s3Path")
            .secretKey("secretKey")
            .secretId("secretID")
            .done()
            .buildRepresentationConnection();


    var connection2 = new ConnectionBuilder()
            .name("file")
            .file()
            .path("filePath")
            .done()
            .buildRepresentationConnection();

    var tables = new Table[2];
    tables[0] = table1;
    tables[1] = table2;

    var connections = new Connection[2];
    connections[0] = connection1;
    connections[1] = connection2;

    var dag = new DagBuilder()
            .tables(tables)
            .connections(connections)
            .build();

    logger.info(dag);

    assertEquals(2, dag.getTables().length);
    assertEquals(2, dag.getConnections().length);

    assertEquals("TableTable", dag.getTables()[0].getName());
    assertEquals("TableArea", dag.getTables()[0].getArea());
    assertEquals("TableArea.TableVertical.TableTable.TableVersion", dag.getTables()[0].getClassName());
    assertEquals(4, dag.getTables()[0].getPartitionSize());

    assertEquals("TableTable", dag.getTables()[1].getName());
    assertEquals("somedifferent", dag.getTables()[1].getArea());
    assertEquals("my.class.name", dag.getTables()[1].getClassName());
    assertEquals(4, dag.getTables()[1].getPartitionSize());



  }

  @Test
  void nestedBuildTest2() {

    Table table1 = new TableBuilder()
            .area("TableArea")
            .vertical("TableVertical")
            .table("TableTable")
            .version("TableVersion")
            .parallelism(2)
            .maxBulkSize(2)
            .source()
            .connectionName("SourceConnection1")
            .path("SourcePath1")
            .format("jdbc")
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
            .area("DepArea0")
            .version("0.1")
            .vertical("DepVertical0")
            .table("DepTableTable0")
            .format("jdbc")
            .done()
            .dependency()
            .area("DepArea1")
            .version("1.1")
            .vertical("DepVertical1")
            .table("DepTableTable1")
            .format("jdbc")
            .transform()
            .type("identity")
            .done()
            .done()
            .dependency()
            .area("DepArea2")
            .version("1.2")
            .vertical("DepVertical2")
            .table("DepTableTable2")
            .format("jdbc")
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
            .format("jdbc")
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
            .format("jdbc")
            .transform()
            .type("aggregate")
            .partition()
            .size(4)
            .unit("hours")
            .done()
            .done()
            .done()
            .buildRepresentationTable();


    var partition =
            new PartitionBuilder<>()
                    .unit("days")
                    .size(10)
                    .build();

    Table table2 = new Table();
    table2.setTargets(table1.getTargets());
    table2.setDependencies(table1.getDependencies());
    table2.setSources(table1.getSources());
    table2.setPartition(partition);
    table2.setMax_bulk_size(table1.getMax_bulk_size());
    table2.setParallelism(table1.getParallelism());
    table2.setVersion(table1.getVersion());
    table2.setVertical(table1.getVertical());
    table2.setTable(table1.getTable());
    table2.setArea("somedifferent");
    table2.setClass_name("my.class.name");

    var connection1 = new ConnectionBuilder()
            .name("s3")
            .s3()
            .path("s3Path")
            .secretKey("secretKey")
            .secretId("secretID")
            .done()
            .buildRepresentationConnection();


    var connection2 = new ConnectionBuilder()
            .name("file")
            .file()
            .path("filePath")
            .done()
            .buildRepresentationConnection();

    var tables = new Table[2];
    tables[0] = table1;
    tables[1] = table2;

      var connections = new Connection[2];
      connections[0] = connection1;
      connections[1] = connection2;

      var dag = new DagBuilder()
              .tables(tables)
              .connections(connections)
              .build();

      logger.info(dag);
      assertFalse(dag.getTables()[0].isPartitioned());
      assertTrue(dag.getTables()[1].isPartitioned());
      assertTrue(dag.getTables()[0].getSources()[0].isPartitioned());
      assertFalse(dag.getTables()[0].getSources()[1].isPartitioned());
      assertEquals(PartitionTransformType.NONE, dag.getTables()[1].getDependencies()[0].getTransformType());
      assertEquals(PartitionTransformType.IDENTITY, dag.getTables()[1].getDependencies()[1].getTransformType());

      assertEquals("TableTable", dag.getTables()[0].getName());
      assertEquals("TableArea", dag.getTables()[0].getArea());
      assertEquals("TableArea.TableVertical.TableTable.TableVersion", dag.getTables()[0].getClassName());
      assertEquals(0, dag.getTables()[0].getPartitionSize());

      assertEquals("TableTable", dag.getTables()[1].getName());
      assertEquals("somedifferent", dag.getTables()[1].getArea());
    assertEquals("my.class.name", dag.getTables()[1].getClassName());
    assertEquals(10, dag.getTables()[1].getPartitionSize());

    assertEquals("s3", dag.getConnections()[0].getName());
    assertEquals("file", dag.getConnections()[1].getName());

  }

}
