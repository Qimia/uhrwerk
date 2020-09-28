package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent, TableIdent}
import org.apache.spark.sql.DataFrame

case class TaskInput(loadedInputFrames: Map[Ident, DataFrame],
                     notLoadedSources: Map[SourceIdent, DependentLoaderSource]) {

  /**
   * Retrieve a DataFrame easier by just giving a name
   * @param inputName name of the Table or path of the source
   * @return DataFrame of the first match found
   */
  def getFrameByName(inputName: String): DataFrame = {
    loadedInputFrames
      .filter(tup =>
        tup._1 match {
          case TableIdent(_, _, name, _) => name == inputName
          case SourceIdent(_, path, _)   => path == inputName
      })
      .values
      .head
  }

  /**
   * Retrieve a DataFrame easier by only giving the area / vertical / name
   * of a dependency. Does not work for sources.
   * @param area dependency's area
   * @param vertical dependency's vertical
   * @param name dependency's (table) name
   * @return Dataframe of the first match found
   */
  def getTableFrame(area: String, vertical: String, name: String): DataFrame = {
    loadedInputFrames
      .filter(tup =>
        tup._1 match {
          case TableIdent(areaI, verticalI, nameI, _) =>
            (areaI == area) && (verticalI == vertical) && (nameI == name)
          case SourceIdent(_, _, _) => false
      })
      .values
      .head
  }
}
