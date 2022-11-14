package io.qimia.uhrwerk.framemanager

import io.qimia.uhrwerk.common.model.{DependencyModel, PartitionTransformType, TableModel, TargetModel}

object Converters {

  def convertTargetToDependency(target: TargetModel, table: TableModel): DependencyModel = {
    val dependency = DependencyModel.builder().build()
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
