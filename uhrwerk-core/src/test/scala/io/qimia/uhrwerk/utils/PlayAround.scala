package io.qimia.uhrwerk.utils

import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec

class PlayAround extends AnyFlatSpec {

  "Given a premade global config it" should "be parsable by the configreader" in {
    println("test")

    val confPath = Paths.get(getClass.getResource("/config/test_yml/table_test_1.yml").getPath)
    val stepConf = ConfigReader.readStepConfig(confPath)

  }

}

