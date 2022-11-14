package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class YamlConfigReaderTest {
  private final Logger logger = LoggerFactory.getLogger(TransformBuilderTest.class);

  @Test
  public void readConnectionsTest() {
    ConnectionModel[] Connections =
        (new YamlConfigReader()).readConnections("config/connection-config.yml");
    for (ConnectionModel conn : Connections) {
      logger.info(conn.toString());
    }
    assertEquals(Connections[0].getName(), "mysql1");
    assertEquals(Connections[1].getName(), "s3_test");
    assertEquals(Connections[2].getName(), "local_filesystem_test");
    assertEquals(Connections[0].getJdbcUrl(), "jdbc:mysql://localhost:3306");
    assertNull(Connections[1].getJdbcUrl());
    assertNull(Connections[2].getJdbcUrl());
    assertEquals(Connections[0].getJdbcDriver(), "com.mysql.jdbc.Driver");
    assertNull(Connections[1].getJdbcDriver());
    assertNull(Connections[2].getJdbcDriver());
    assertEquals(Connections[0].getJdbcUser(), "root");
    assertNull(Connections[1].getJdbcUser());
    assertNull(Connections[2].getJdbcUser());
    assertEquals(Connections[0].getJdbcPass(), "mysql");
    assertNull(Connections[1].getJdbcPass());
    assertNull(Connections[2].getJdbcPass());
    assertNull(Connections[0].getPath());
    assertEquals(Connections[1].getPath(), "s3://bucketname/somesuffix/");
    assertEquals(Connections[2].getPath(), "/path/to/local/datalake");
    assertNull(Connections[0].getAwsAccessKeyID());
    assertEquals(Connections[1].getAwsAccessKeyID(), "blabla");
    assertNull(Connections[2].getAwsAccessKeyID());
    assertNull(Connections[0].getAwsSecretAccessKey());
    assertEquals("yaya", Connections[1].getAwsSecretAccessKey());
    assertNull(Connections[2].getAwsSecretAccessKey());
  }

  @Test
  public void readDagTest() {

    DagModel dag = (new YamlConfigReader()).readDag("config/dag-config.yml");
    logger.info(dag.toString());
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
    DagModel dag = (new YamlConfigReader()).readDag("s3://qimia-aws-test/uhrwerk/dag-config.yml");
    assertEquals("load.class.name", dag.getTables()[0].getClassName());
    assertFalse(dag.getTables()[1].isPartitioned());
  }

  @Test
  @Disabled
  public void azureBlobConfigReadTest() {
    DagModel dag =
        (new YamlConfigReader())
            .readDag(
                "https://uhrwerkazureblobtest.blob.core.windows.net/configs/dag-config.yml;DefaultEndpointsProtocol=https;AccountName=uhrwerkazureblobtest;AccountKey=<insert_key>;EndpointSuffix=core.windows.net");
    assertEquals("load.class.name", dag.getTables()[0].getClassName());
    assertFalse(dag.getTables()[1].isPartitioned());
  }

  @Test
  public void readEnvTest() {
    MetastoreModel metastore = (new YamlConfigReader()).readEnv("config/env-config.yml");
    logger.info(metastore.toString());
    assertEquals(metastore.getJdbc_url(), "jdbc:mysql://localhost:53306/UHRWERK_METASTORE");
    assertEquals(metastore.getJdbc_driver(), "com.mysql.jdbc.Driver");
    assertEquals(metastore.getUser(), "UHRWERK_USER");
    assertEquals(metastore.getPass(), "Xq92vFqEKF7TB8H9");
  }

  @Test
  public void readTablesTest() {
    TableModel table = (new YamlConfigReader()).readTable("config/table1-config.yml");
    logger.info(table.toString());
    assertEquals(table.getId(), table.getSources()[0].getTableId());
    assertEquals(table.getId(), table.getTargets()[0].getTableId());
    assertEquals(table.getId(), table.getDependencies()[0].getTableId());
    assertNotEquals(
        table.getDependencies()[0].getDependencyTargetId(), table.getTargets()[0].getId());
  }
}
