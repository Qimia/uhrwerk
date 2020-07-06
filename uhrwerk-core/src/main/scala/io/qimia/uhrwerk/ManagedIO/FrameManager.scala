package io.qimia.uhrwerk.ManagedIO

// Trait that describes how to read and write dataframes to and from the datalake/datawarehouse
trait FrameManager {
  // There could be a spark / iceberg / deltalake / hudi version depending on what the user wants to use

  def loadDFFromLake()

  def writeDFToLake()

  // The generic write to jdbc part that is not connected to the datalake could already be implemented here

  def loadDFFromJdbc()

  def writeDFToJdbc()
}
