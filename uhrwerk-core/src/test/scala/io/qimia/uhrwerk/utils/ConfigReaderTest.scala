package io.qimia.uhrwerk.utils

import java.nio.file.Paths

import io.qimia.uhrwerk.models.config.{Global, Table}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigReaderTest extends AnyFlatSpec {

  "Given a premade global config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/global_test_1.yml").getPath)
    val gconf = ConfigReader.readGlobalConfig(confPath)

    val connections = gconf.getConnections()

    val predictedConnectionNames = "mysql_test" :: "s3_test" :: "local_filesystem_test" :: Nil
    connections.zip(predictedConnectionNames).foreach((tup) => tup._1.getName === tup._2)
  }

  "Given a premade step config it" should "be parsable by the configreader" in {
    val confPath = Paths.get(getClass.getResource("/config/table_test_1.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getName === "load_a_table")
    assert(stepConf.getBatchSize === "6h")
    assert(stepConf.getParallelism === 10)
    assert(stepConf.getVersion == 1)  // Check default init
    assert(stepConf.getSources == null)
    val deps = stepConf.getDependencies
    val predictedPaths = "schema.table" :: "someplace/other_table" :: Nil
    deps.zip(predictedPaths).foreach((tup) => {
      assert(tup._1.getPath === tup._2)
      assert(tup._1.getArea === "staging")
    })
  }

  it should "be parsable with sources instead of dependencies" in {
    val confPath = Paths.get(getClass.getResource("/config/table_test_2.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getName === "dump_a_table")
    assert(stepConf.getBatchSize === "1h")
    assert(stepConf.getParallelism === 1)
    assert(stepConf.getVersion == 1)  // Check default init
    assert(stepConf.getDependencies == null)
    val source = stepConf.getSources.head
    assert(source.getPath === "schema.staging_source_table")
    assert(source.getPartitionQuery === "SELECT id FROM <path> WHERE created_at >= <lower_bound> and created_at < <upper_bound>")
    assert(source.getQueryColumn === "created_at")
    assert(source.getSelectQuery === "SELECT * FROM <path> WHERE created_at >= <lower_bound> AND created_at < <upper_bound>")
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
