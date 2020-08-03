package io.qimia.uhrwerk.utils

import java.nio.file.Paths

import io.qimia.uhrwerk.models.config.{Global, Step}
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
    val confPath = Paths.get(getClass.getResource("/config/step_test_1.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

    assert(stepConf.getName === "load_a_table")
    assert(stepConf.getBatchSize === "6h")
    assert(stepConf.getParallelism === 10)
    assert(stepConf.getVersion == 1)  // Check default init
    val deps = stepConf.getDependencies
    val predictedPaths = "schema.table" :: "someplace/other_table" :: Nil
    deps.zip(predictedPaths).foreach((tup) => {
      assert(tup._1.getPath === tup._2)
      assert(tup._1.getArea === "staging")
    })
  }
}
