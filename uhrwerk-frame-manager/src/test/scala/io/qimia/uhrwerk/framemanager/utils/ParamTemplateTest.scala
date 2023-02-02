package io.qimia.uhrwerk.framemanager.utils

import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties

import scala.collection.JavaConverters._

class ParamTemplateTest extends AnyFlatSpec {
  "Properties Reading" should "work" in {
    val properties = new Properties()
    properties.load(getClass.getResourceAsStream("/properties/test.properties"))
    assert(properties.getProperty("some_variable") == "some_value")

    val args = Array("some_variable", "some_other_variable")
      .flatMap(vr => {
        val propVal = properties.getProperty(vr)
        if (propVal != null && propVal.nonEmpty) {
          Some((vr, propVal))
        } else
          None
      })
      .toMap

    assert(args("some_variable") == "some_value")

    assert(args.asJava.get("some_variable") == "some_value")

  }

}
