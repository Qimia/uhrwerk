package io.qimia.uhrwerk

import org.apache.spark.sql.DataFrame

trait UhrwerkTask {
  def process(deps: Map[(String, String, String), DataFrame]):DataFrame
}
