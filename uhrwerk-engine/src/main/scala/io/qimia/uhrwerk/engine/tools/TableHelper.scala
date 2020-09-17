package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model.Table
import io.qimia.uhrwerk.engine.Environment.TableIdent

object TableHelper {

  /**
    * Create TableIdentity from a table object
    * @param table table which needs an ident
    * @return a table identifier
    */
  def extractTableIdentity(table: Table): TableIdent =
    TableIdent(table.getArea, table.getVertical, table.getName, table.getVersion)

}
