package io.qimia.uhrwerk.utils

import java.nio.file.Paths
import java.time.Duration

import io.qimia.uhrwerk.config.representation.{Global, Config, Table, Connection}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigReaderTest extends AnyFlatSpec {

  "Given a complete dag yaml" should "be parsable by the configreader" in {
    val completePath = Paths.get(getClass.getResource("/config/complete_dag.yml").getPath)
    val complete = ConfigReader.readComplete(completePath)

    val connections = complete.getConfig.getConnections
    val metastore = complete.getUhrwerk.getMetastore
    val tables = complete.getTables

    val predictedConnectionNames = "mysql1" :: "s3_test" :: "local_filesystem_test" :: Nil
    connections.zip(predictedConnectionNames).foreach((tup) => tup._1.getName === tup._2)

    assert(metastore.getJdbc_url === "jdbc:mysql://localhost:53306/UHRWERK_METASTORE")

    assert(tables.head.getArea === "processing")


  }



  "Given a premade global config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/global_test_1.yml").getPath)
    val gconf = ConfigReader.readGlobalConfig(confPath)

    val connections = gconf.getConfig().getConnections()
    val metastore = gconf.getUhrwerk().getMetastore()


    val predictedConnectionNames = "mysql1" :: "s3_test" :: "local_filesystem_test" :: Nil
    connections.zip(predictedConnectionNames).foreach((tup) => tup._1.getName === tup._2)

    assert(connections(0).getJdbc.getJdbc_url === "jdbc:mysql://localhost:3306")
    assert(connections(1).getS3.getSecret_id === "blabla")
    assert(connections(2).getFile.getPath === "/path/to/local/datalake")

    assert(metastore.getJdbc_url === "jdbc:mysql://localhost:53306/UHRWERK_METASTORE")
    assert(metastore.getJdbc_driver === "com.mysql.jdbc.Driver")
    assert(metastore.getUser === "UHRWERK_USER")
    assert(metastore.getPass === "Xq92vFqEKF7TB8H9")

  }

  "Given a premade step config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/table_test_1.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getTable === "load_a_table")
    assert(stepConf.getParallelism === 10)
    assert(stepConf.getVersion == "1.0")  // Check default init
    assert(stepConf.getArea === "processing")
    assert(stepConf.getMax_bulk_size === 12)
    assert(stepConf.getSources()(0).getConnection_name == "connection_name")
    val deps = stepConf.getDependencies
    val predictedType = "identity" :: "aggregate" :: "window" :: "temporal_aggregate" :: Nil
    deps.zip(predictedType).foreach((tup) => {
      assert(tup._1.getTransform.getType === tup._2)
      assert(tup._1.getConnection_name === "connection_name")
    })

  }


  it should "be parsable with sources instead of dependencies" in {
    val confPath = Paths.get(getClass.getResource("/config/table_test_2.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getTable === "dump_a_table")
    assert(stepConf.getPartition.getSize === 4)
    //assert(stepConf.getPartitionSizeDuration === Duration.ofHours(6))
    assert(stepConf.getParallelism === 1)
    assert(stepConf.getVersion == "1.0")  // Check default init
    assert(stepConf.getDependencies == null)
    val source = stepConf.getSources.head
    assert(source.getFormat === "jdbc")
    assert(source.getPath === "staging_source_table")
    assert(source.getParallel_load.getQuery === "SELECT id FROM <path> WHERE created_at >= '<lower_bound>' and created_at \\< '<upper_bound>'")
    assert(source.getSelect.getColumn === "created_at")
    assert(source.getSelect.getQuery === "table_test_2_select_query.sql")
    assert(source.getParallel_load.getNum == 40)
  }


  "readQueryFile with a bad input" should "Read nothing and show error" in {
    val out = ConfigReader.readQueryFile("a/b/c/file_not_exists.sql")
    assert (out === "FILEQUERY FAILED")
  }

  "readQueryFile with a valid query file location" should "read and return the given query" in {
    val out = ConfigReader.readQueryFile(getClass.getResource("/config/table_test_2_select_query.sql").getPath)
    assert (out === "SELECT * FROM <path> WHERE created_at >= <lower_bound> AND created_at < <upper_bound>")
  }

}
