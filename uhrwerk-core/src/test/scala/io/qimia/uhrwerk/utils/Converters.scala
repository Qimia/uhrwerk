package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.models.config.{Dependency, Target}

object Converters {

  def convertTargetToDependency(t: Target): Dependency = {
    val d = new Dependency
    d.setArea(t.getArea)
    d.setConnectionName(t.getConnectionName)
    d.setPartitionSize(t.getPartitionSize)
    d.setPath(t.getPath)
    d.setVersion(t.getVersion)
    d.setVertical(t.getVertical)
    d
  }

}
