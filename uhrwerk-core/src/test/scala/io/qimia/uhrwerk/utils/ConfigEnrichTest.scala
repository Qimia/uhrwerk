package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.models.config.{Dependency, Source, Step, Target}
import org.scalatest.flatspec.AnyFlatSpec

class ConfigEnrichTest extends AnyFlatSpec {

  "given a single batchsize all unknown partitioned tables" should "use the step's batchsize" in {
    val someStep = new Step()
    someStep.setName("some_step")
    someStep.setBatchSize("30m")
    val sourceA = new Source
    val sources = Array(sourceA)
    someStep.setSources(sources)
    val dependencyA = new Dependency
    val dependencyB = new Dependency
    dependencyB.setPartitionSize("15m")
    val dependencies = Array(dependencyA, dependencyB)
    someStep.setDependencies(dependencies)
    val targetA = new Target
    val targets = Array(targetA)
    someStep.setTargets(targets)
    ConfigEnrich.autofillStepPartitionSizes(someStep)

    assert(someStep.getBatchSize == "30m")
    assert(sourceA.getPartitionSize == "30m")
    assert(dependencyA.getPartitionSize == "30m")
    assert(dependencyB.getPartitionSize == "15m")
    assert(targetA.getPartitionSize == "30m")
  }

}
