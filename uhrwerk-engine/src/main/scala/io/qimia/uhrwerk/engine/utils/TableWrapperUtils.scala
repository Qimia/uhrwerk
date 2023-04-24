package io.qimia.uhrwerk.engine.utils

import io.qimia.uhrwerk.engine.TableFunction
import org.apache.log4j.Logger

import scala.collection.mutable

object TableWrapperUtils {
  private val logger: Logger = Logger.getLogger(TableWrapperUtils.getClass)

  def getTableFunction(className: String): TableFunction =
    // Dynamically load the right class and return the function described in it
    // either it is defined in the table object or through the convention of classnaming
    Class
      .forName(className)
      .getConstructor()
      .newInstance()
      .asInstanceOf[TableFunction]

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
        val propName = lookUpMap(partitionColumn, partitionMappings) match {
          case Some(value) => propertyName(value)
          case None        => partitionColumn
        }
        lookUpMap(propName, properties) match {
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

  private def propertyName(value: AnyRef): String = {
    if (value.isInstanceOf[String]) {
      val valueStr = value.asInstanceOf[String]
      if (valueStr.startsWith("$") && valueStr.endsWith("$")) {
        valueStr.substring(1, valueStr.length - 1)
      } else {
        valueStr
      }
    } else {
      value.toString
    }
  }

  private def lookUpMap(
      key: String,
      map: mutable.Map[String, AnyRef]
  ): Option[AnyRef] = {
    if (map != null) map.get(key) else None
  }
}
