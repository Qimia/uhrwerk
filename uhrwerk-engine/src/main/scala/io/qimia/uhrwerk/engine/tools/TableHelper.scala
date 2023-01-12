package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.metastore.model.TableModel
import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent, TableIdent}

object TableHelper {

  /** Create TableIdentity from a table object
    * @param table table which needs an ident
    * @return a table identifier
    */
  def extractTableIdentity(table: TableModel): TableIdent =
    new TableIdent(
      table.getArea,
      table.getVertical,
      table.getName,
      table.getVersion
    )

  /** Find a particular table in a sequence of tables
    * (Assumes that there is only a single version of a table in the sequence)
    * @param area table's area
    * @param vertical table's vertical
    * @param name table's name
    * @param tables sequence of tables
    * @return Identifier of the table
    */
  def findIdent(
      area: String,
      vertical: String,
      name: String,
      tables: Seq[Ident]
  ): TableIdent = {
    tables
      .filter(id => {
        if (id.isInstanceOf[TableIdent]) {
          val ident = id.asInstanceOf[TableIdent]
          (ident.area == area) && (ident.vertical == vertical) && (ident.name == name)
        } else false
      })
      .head
      .asInstanceOf[TableIdent]
  }

}
