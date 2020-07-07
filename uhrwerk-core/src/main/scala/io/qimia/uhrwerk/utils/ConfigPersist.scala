package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.models.config.Step
import io.qimia.uhrwerk.models.store.StepConfig
import javax.persistence.EntityManager

object ConfigPersist {

  def stepToStepStoreObj(step: Step): StepConfig = new StepConfig(
    step.getName,
    TimeTools.convertDurationToObj(step.getBatchSize),
    step.getParallelism,
    step.getMaxBatches,
    step.getStepType
  )

  def persistStep(store: EntityManager, step: Step): StepConfig = {

    // Need to find out which TableInfo are already in the storage and re-use
    // those if possible

    // Need to find out when to write a new row of configuration (preferably
    // we reference the old one if nothing has changed, but does this comparison lead
    // to extra requests and less performance?)

    null
  }

}
