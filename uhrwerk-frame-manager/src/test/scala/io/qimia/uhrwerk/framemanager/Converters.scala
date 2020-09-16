package io.qimia.uhrwerk.framemanager

import io.qimia.uhrwerk.common.model.{Dependency, PartitionTransformType, Table, Target}

object Converters {

  def convertTargetToDependency(target: Target, table: Table): Dependency = {
    val dependency = new Dependency
    dependency.setArea(table.getArea)
    dependency.setFormat(target.getFormat)
    dependency.setVersion(table.getVersion)
    dependency.setVertical(table.getVertical)
    dependency.setTableName(table.getName)
    dependency.setTransformPartitionSize(table.getPartitionSize)
    dependency.setTransformPartitionUnit(table.getPartitionUnit)
    dependency.setTransformType(PartitionTransformType.IDENTITY)
    dependency
  }

}
