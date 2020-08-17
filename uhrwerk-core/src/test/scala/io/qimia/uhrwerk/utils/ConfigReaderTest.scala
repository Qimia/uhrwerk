package io.qimia.uhrwerk.utils

import java.nio.file.Paths
import java.time.Duration

import io.qimia.uhrwerk.config.representation.{Global, Config, Table, Connection}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigReaderTest extends AnyFlatSpec {

  "Given a complete dag yaml" should "be parsable by the configreader" in {
    val completePath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag.yml").getPath)
    val complete = ConfigReader.readComplete(completePath)

    val connections = complete.getGlobal.getConfig.getConnections
    val metastore = complete.getGlobal.getUhrwerk.getMetastore
    val tables = complete.getTables

    assert(metastore.getJdbc_url === "jdbc:mysql://localhost:53306/UHRWERK_METASTORE")
    assert(metastore.getJdbc_driver === "com.mysql.jdbc.Driver")
    assert(metastore.getUser === "UHRWERK_USER")
    assert(metastore.getPass === "Xq92vFqEKF7TB8H9")


    val predictedConnectionNames = "mysql1" :: "s3_test" :: "local_filesystem_test" :: Nil
    connections.zip(predictedConnectionNames).foreach((tup) => tup._1.getName === tup._2)

    assert(connections(0).getJdbc.getJdbc_url === "jdbc:mysql://localhost:3306")
    assert(connections(0).getS3 === null)
    assert(connections(0).getFile === null)

    assert(connections(1).getJdbc === null)
    assert(connections(1).getS3.getPath === "s3://bucketname/somesuffix/")
    assert(connections(1).getFile === null)

    assert(connections(2).getJdbc === null)
    assert(connections(2).getS3 === null)
    assert(connections(2).getFile.getPath === "/path/to/local/datalake")


    assert(tables(0).getArea === "processing")
    assert(tables(0).getVertical === "sourcedb_1")
    assert(tables(0).getDependencies() === null)
    assert(tables(0).getSources.head.getConnection_name === "connection_name")
    assert(tables(0).getTargets.head.getConnection_name === "connection_name2")

    assert(tables(1).getArea === "processing")
    assert(tables(1).getVertical === "sourcedb_1")
    assert(tables(1).getSources() === null)
    assert(tables(1).getDependencies.head.getConnection_name === "connection_name")
    assert(tables(1).getTargets.head.getConnection_name === "connection_name2")

  }

  "Given a complete dag yaml with errors" should "throw errors" in {

    //The following should throw an error, because there is a field misspelled (xuser instead of user)
    assertThrows[org.yaml.snakeyaml.error.YAMLException] {
      val completePath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_field_misspelled.yml").getPath)
      val complete = ConfigReader.readComplete(completePath)
    }
    //The following should throw an error, because there is a type mismatch (bulk_size is float instead of int)
    assertThrows[org.yaml.snakeyaml.constructor.ConstructorException] {
      val completePath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_type_mismatch.yml").getPath)
      val complete = ConfigReader.readComplete(completePath)
    }
    //The following should throw an error, because there is a wrong indent as jdbc_url
    assertThrows[org.yaml.snakeyaml.parser.ParserException] {
      val completePath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_wrong_indent.yml").getPath)
      val complete = ConfigReader.readComplete(completePath)
    }

    //The following should throw an error, because there is no minus in connections
    assertThrows[io.qimia.uhrwerk.config.ConfigException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_no_minus.yml").getPath)
      val stepConf = ConfigReader.readComplete(confPath)
    }

    //The following should throw an error, because there is no file like that
    assertThrows[java.lang.NullPointerException] {
      val completePath = Paths.get(getClass.getResource("/config/test_yml/not_existing.yml").getPath)
      val complete = ConfigReader.readComplete(completePath)
    }

    //The following should throw an error, because there is the config entry missing
    assertThrows[io.qimia.uhrwerk.config.ConfigException] {
      val completePath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_missing_fields.yml").getPath)
      val complete = ConfigReader.readComplete(completePath)
    }

    //The following should throw an error, because there is no source and no dependency defined
    assertThrows[io.qimia.uhrwerk.config.ConfigException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_no_source_no_dep.yml").getPath)
      val stepConf = ConfigReader.readComplete(confPath)
    }

    //The following should throw an error, because there is no target defined
    assertThrows[io.qimia.uhrwerk.config.ConfigException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/complete_test_dag_no_target.yml").getPath)
      val stepConf = ConfigReader.readComplete(confPath)
    }
  }

  "Given a premade global config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/test_yml/global_test_1.yml").getPath)
    val gconf = ConfigReader.readGlobalConfig(confPath)

    val connections = gconf.getConfig().getConnections()
    val metastore = gconf.getUhrwerk().getMetastore()

    assert(metastore.getJdbc_url === "jdbc:mysql://localhost:53306/UHRWERK_METASTORE")
    assert(metastore.getJdbc_driver === "com.mysql.jdbc.Driver")
    assert(metastore.getUser === "UHRWERK_USER")
    assert(metastore.getPass === "Xq92vFqEKF7TB8H9")

    val predictedConnectionNames = "mysql1" :: "s3_test" :: "local_filesystem_test" :: Nil
    connections.zip(predictedConnectionNames).foreach((tup) => tup._1.getName === tup._2)

    assert(connections(0).getJdbc.getJdbc_url === "jdbc:mysql://localhost:3306")
    assert(connections(1).getS3.getSecret_id === "blabla")
    assert(connections(2).getFile.getPath === "/path/to/local/datalake")

    assert(connections(0).getJdbc.getJdbc_url === "jdbc:mysql://localhost:3306")
    assert(connections(0).getJdbc.getJdbc_driver === "com.mysql.jdbc.Driver")
    assert(connections(0).getJdbc.getUser === "root")
    assert(connections(0).getJdbc.getPass === "mysql")
    assert(connections(0).getS3 === null)
    assert(connections(0).getFile === null)

    assert(connections(1).getJdbc === null)
    assert(connections(1).getS3.getPath === "s3://bucketname/somesuffix/")
    assert(connections(1).getS3.getSecret_id === "blabla")
    assert(connections(1).getS3.getSecret_key === "yaya")
    assert(connections(1).getFile === null)

    assert(connections(2).getJdbc === null)
    assert(connections(2).getS3 === null)
    assert(connections(2).getFile.getPath === "/path/to/local/datalake")

  }

  "Given a premade global config with errors it" should "throw errors" in {

    //Should throw an error because there is no connection like s3, jdbc or file filled
    assertThrows[org.yaml.snakeyaml.constructor.ConstructorException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/global_test_no_connection.yml").getPath)
      val gconf = ConfigReader.readGlobalConfig(confPath)
    }

    //Should throw an error because there is no connection like s3, jdbc or file filled
    assertThrows[org.yaml.snakeyaml.constructor.ConstructorException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/global_test_no_connection.yml").getPath)
      val gconf = ConfigReader.readGlobalConfig(confPath)
    }

    //Should throw an error because there are two connections jdbc and file filled
    assertThrows[org.yaml.snakeyaml.constructor.ConstructorException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/global_test_several_connections.yml").getPath)
      val gconf = ConfigReader.readGlobalConfig(confPath)
    }

    //Should throw an error because there are two connections jdbc and file filled
    assertThrows[org.yaml.snakeyaml.constructor.ConstructorException] {
      val confPath = Paths.get(getClass.getResource("/config/test_yml/global_test_several_connections.yml").getPath)
      val gconf = ConfigReader.readGlobalConfig(confPath)
    }

  }

  "Given a premade step config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/test_yml/table_test_1.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    println(stepConf.getSources)

    assert(stepConf.getTable === "load_a_table")
    assert(stepConf.getParallelism === 10)
    assert(stepConf.getVersion == "1.0")  // Check default init
    assert(stepConf.getArea === "processing")
    assert(stepConf.getMax_bulk_size === 12)
    assert(stepConf.getSources()(0).getConnection_name == "connection_name1")
    val deps = stepConf.getDependencies
    val predictedType = "identity" :: "aggregate" :: "window" :: "temporal_aggregate" :: Nil
    deps.zip(predictedType).foreach((tup) => {
      assert(tup._1.getTransform.getType === tup._2)
      assert(tup._1.getConnection_name === "connection_name")
    })
    assert(deps(3).getTransform.getPartition.getSize === 1)

  }


  it should "be parsable with sources instead of dependencies" in {
    val confPath = Paths.get(getClass.getResource("/config/test_yml/table_test_2_no_dependency.yml").getPath)
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
    assert(source.getSelect.getQuery === "SELECT * FROM <path> WHERE created_at >= <lower_bound> AND created_at \\< <upper_bound>")
    assert(source.getParallel_load.getNum == 40)
  }

  it should "be parsable with dependencies instead of sources" in {
    val confPath = Paths.get(getClass.getResource("/config/test_yml/table_test_3_no_source.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getSources === null)
    assert(stepConf.getDependencies.head.getTransform.getType === "identity")
  }

  "readQueryFile with a bad input" should "Read nothing and show error" in {
    val out = ConfigReader.readQueryFile("a/b/c/file_not_exists.sql")
    assert (out === "FILEQUERY FAILED")
  }

  "readQueryFile with a valid query file location" should "read and return the given query" in {
    val out = ConfigReader.readQueryFile(getClass.getResource("/config/test_yml/table_test_2_select_query.sql").getPath)
    assert (out === "SELECT * FROM <path> WHERE created_at >= <lower_bound> AND created_at \\< <upper_bound>")
  }

}
