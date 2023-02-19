package io.qimia.uhrwerk.engine.utils

import org.apache.log4j.Logger

import scala.collection.mutable

object TableWrapperUtils {
  private val logger: Logger = Logger.getLogger(TableWrapperUtils.getClass)
  def getPartitionValues(
      partitionColumns: Seq[String],
      partitionMappings: mutable.Map[String, AnyRef],
      properties: mutable.Map[String, AnyRef]
  ): mutable.Map[String, AnyRef] = {
    val partitionValues = mutable.LinkedHashMap[String, AnyRef]()
    if (
      partitionColumns != null &&
      partitionColumns.nonEmpty
    ) {
      for (partitionColumn <- partitionColumns) {
        val propKey = lookUpMap(partitionColumn, partitionMappings) match {
          case Some(value) => value.toString
          case None        => partitionColumn
        }
        lookUpMap(propKey, properties) match {
          case Some(value) => partitionValues.put(partitionColumn, value)
          case None =>
            val errorMessage =
              s"Partition column $partitionColumn is not in the frame and no property value is given for it."
            logger.error(
              errorMessage
            )
            throw new IllegalArgumentException(errorMessage)
        }
      }
    }
    partitionValues
  }

  private def lookUpMap(
      key: String,
      map: mutable.Map[String, AnyRef]
  ): Option[AnyRef] = {
    if (map != null) map.get(key) else None
  }
}
