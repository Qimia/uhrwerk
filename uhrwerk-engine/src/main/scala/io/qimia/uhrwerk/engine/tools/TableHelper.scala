package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.framemanager.BulkDependencyResult
import io.qimia.uhrwerk.common.model.Table
import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent, TableIdent}

object TableHelper {

  /**
    * Create TableIdentity from a table object
    * @param table table which needs an ident
    * @return a table identifier
    */
  def extractTableIdentity(table: Table): TableIdent =
    TableIdent(table.getArea, table.getVertical, table.getName, table.getVersion)

  /**
    * Find a particular table in a sequence of tables
    * (Assumes that there is only a single version of a table in the sequence)
    * @param area table's area
    * @param vertical table's vertical
    * @param name table's name
    * @param tables sequence of tables
    * @return Identifier of the table
    */
  def findIdent(area: String, vertical: String, name: String, tables: Seq[Ident]): TableIdent = {
    tables
      .filter(id =>
        id match {
          case TableIdent(areaI, verticalI, nameI, _) =>
            ((areaI == area) && (verticalI == vertical) && (nameI == name))
          case SourceIdent(_, _, _) => false
      })
      .head
      .asInstanceOf[TableIdent]
  }

}
