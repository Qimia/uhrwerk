package io.qimia.uhrwerk.utils

import java.io.FileInputStream
import java.nio.file.Path

import io.qimia.uhrwerk.models.config.{Global, Step}
import io.qimia.uhrwerk.models.store.StepConfig
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.representer.Representer

import scala.reflect.ClassTag

object ConfigReader {

  // Read a global yaml-file configuration
  def readGlobalConfig(path: Path): Global = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val yaml   = new Yaml(new Constructor(classOf[Global]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Global]
    config
  }

  // read a step yaml-file configuration
  def readStepConfig(path: Path): Step = {
    val fileStream   = new FileInputStream(path.toFile)
    val representer = new Representer
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val yaml   = new Yaml(new Constructor(classOf[Step]), representer)
    val config = yaml.load(fileStream).asInstanceOf[Step]
    config
  }

}
