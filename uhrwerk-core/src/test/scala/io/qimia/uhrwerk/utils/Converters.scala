package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.config.model.{Dependency, Table, Target}

object Converters {

  def convertTargetToDependency(t: Target, tab: Table): Dependency = {
    val d = new Dependency
    d.setArea(tab.getTargetArea)
    d.setConnectionName(t.getConnectionName)
    d.setPartitionSize(tab.getTargetPartitionSize)
    d.setPath(t.getPath)
    d.setVersion(tab.getTargetVersion)
    d.setVertical(tab.getTargetVertical)
    d
  }

}
