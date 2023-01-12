package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent, TableIdent}
import org.apache.spark.sql.DataFrame

case class TaskInput(
    loadedInputFrames: Map[Ident, DataFrame],
    notLoadedSources: Map[SourceIdent, DependentLoaderSource]
) {

  /** Retrieve a DataFrame easier by just giving a name
    * @param inputName name of the Table or path of the source
    * @return DataFrame of the first match found
    */
  def getFrameByName(inputName: String): DataFrame = {
    loadedInputFrames
      .filter(tup => {
        if (tup._1.isInstanceOf[TableIdent])
          tup._1.asInstanceOf[TableIdent].name == inputName
        else if (tup._1.isInstanceOf[SourceIdent])
          tup._1.asInstanceOf[SourceIdent].path == inputName
        else
          false
      })
      .values
      .head
  }

  /** Retrieve a DataFrame easier by only giving the area / vertical / name
    * of a dependency. Does not work for sources.
    * @param area dependency's area
    * @param vertical dependency's vertical
    * @param name dependency's (table) name
    * @return Dataframe of the first match found
    */
  def getTableFrame(area: String, vertical: String, name: String): DataFrame = {
    loadedInputFrames
      .filter(tup =>
        if (tup._1.isInstanceOf[TableIdent]) {
          val ident = tup._1.asInstanceOf[TableIdent]
          (ident.area == area) && (ident.vertical == vertical) && (ident.name == name)
        } else false
      )
      .values
      .head
  }
}
