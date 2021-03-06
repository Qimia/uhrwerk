package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class YamlConfigReaderTest {
  private final Logger logger = Logger.getLogger(this.getClass());

  @Test
  public void readConnectionsTest() {
    Connection[] connections =
        (new YamlConfigReader()).readConnections("config/connection-config.yml");
    for (Connection conn : connections) {
      logger.info(conn);
    }
    assertEquals(connections[0].getName(), "mysql1");
    assertEquals(connections[1].getName(), "s3_test");
    assertEquals(connections[2].getName(), "local_filesystem_test");
    assertEquals(connections[0].getJdbcUrl(), "jdbc:mysql://localhost:3306");
    assertNull(connections[1].getJdbcUrl());
    assertNull(connections[2].getJdbcUrl());
    assertEquals(connections[0].getJdbcDriver(), "com.mysql.jdbc.Driver");
    assertNull(connections[1].getJdbcDriver());
    assertNull(connections[2].getJdbcDriver());
    assertEquals(connections[0].getJdbcUser(), "root");
    assertNull(connections[1].getJdbcUser());
    assertNull(connections[2].getJdbcUser());
    assertEquals(connections[0].getJdbcPass(), "mysql");
    assertNull(connections[1].getJdbcPass());
    assertNull(connections[2].getJdbcPass());
    assertNull(connections[0].getPath());
    assertEquals(connections[1].getPath(), "s3://bucketname/somesuffix/");
    assertEquals(connections[2].getPath(), "/path/to/local/datalake");
    assertNull(connections[0].getAwsAccessKeyID());
    assertEquals(connections[1].getAwsAccessKeyID(), "blabla");
    assertNull(connections[2].getAwsAccessKeyID());
    assertNull(connections[0].getAwsSecretAccessKey());
    assertEquals("yaya", connections[1].getAwsSecretAccessKey());
    assertNull(connections[2].getAwsSecretAccessKey());
  }

  @Test
  public void readDagTest() {

    Dag dag = (new YamlConfigReader()).readDag("config/dag-config.yml");
    logger.info(dag);
    assertTrue(dag.getTables()[0].isPartitioned());
    assertFalse(dag.getTables()[1].isPartitioned());
    assertTrue(dag.getTables()[0].getSources()[0].isPartitioned());
    assertFalse(dag.getTables()[0].getSources()[1].isPartitioned());
    assertEquals("load.class.name", dag.getTables()[0].getClassName());
    assertEquals("processing.sourcedb_1.load_a_table.1.0", dag.getTables()[1].getClassName());
    assertEquals(
        PartitionTransformType.NONE, dag.getTables()[1].getDependencies()[0].getTransformType());
    assertEquals(
        PartitionTransformType.IDENTITY,
        dag.getTables()[1].getDependencies()[1].getTransformType());
  }

  @Test
  @Disabled
  public void s3ConfigReadTest() {
    Dag dag = (new YamlConfigReader()).readDag("s3://qimia-aws-test/uhrwerk/dag-config.yml");
    assertEquals("load.class.name", dag.getTables()[0].getClassName());
    assertFalse(dag.getTables()[1].isPartitioned());
  }

  @Test
  @Disabled
  public void azureBlobConfigReadTest() {
    Dag dag =
        (new YamlConfigReader())
            .readDag(
                "https://uhrwerkazureblobtest.blob.core.windows.net/configs/dag-config.yml;DefaultEndpointsProtocol=https;AccountName=uhrwerkazureblobtest;AccountKey=<insert_key>;EndpointSuffix=core.windows.net");
    assertEquals("load.class.name", dag.getTables()[0].getClassName());
    assertFalse(dag.getTables()[1].isPartitioned());
  }

  @Test
  public void readEnvTest() {
    Metastore metastore = (new YamlConfigReader()).readEnv("config/env-config.yml");
    logger.info(metastore);
    assertEquals(metastore.getJdbc_url(), "jdbc:mysql://localhost:53306/UHRWERK_METASTORE");
    assertEquals(metastore.getJdbc_driver(), "com.mysql.jdbc.Driver");
    assertEquals(metastore.getUser(), "UHRWERK_USER");
    assertEquals(metastore.getPass(), "Xq92vFqEKF7TB8H9");
  }

  @Test
  public void readTablesTest() {
    Table table = (new YamlConfigReader()).readTable("config/table1-config.yml");
    logger.info(table);
    assertEquals(table.getId(), table.getSources()[0].getTableId());
    assertEquals(table.getId(), table.getTargets()[0].getTableId());
    assertEquals(table.getId(), table.getDependencies()[0].getTableId());
    assertNotEquals(
        table.getDependencies()[0].getDependencyTargetId(), table.getTargets()[0].getId());
  }
}
