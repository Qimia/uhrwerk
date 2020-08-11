package io.qimia.uhrwerk.utils

import io.qimia.uhrwerk.config.model.{Dependency, Table, Target}

object Converters {

  def convertTargetToDependency(t: Target, tab: Table): Dependency = {
    val d = new Dependency
    d.setArea(tab.getArea)
    d.setConnectionName(t.getConnectionName)
    d.setPartitionSize(tab.getPartitionSize)
    d.setFormat(t.getFormat)
    d.setVersion(tab.getVersion)
    d.setVertical(tab.getVertical)
    d
  }

}
