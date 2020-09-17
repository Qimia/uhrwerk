package io.qimia.uhrwerk.engine

import java.time.LocalDateTime

import io.qimia.uhrwerk.common.framemanager.FrameManager
import io.qimia.uhrwerk.common.model.Source
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Special class that allows for the retrieval of dataframes which depend on other dataframes.
  *
  * @param source       Source object from information from the metastore.
  * @param frameManager Frame Manager.
  * @param startTS      Start timestamp for partitioned sources.
  * @param endTSExcl    End timestamp for partitioned sources.
  */
case class DependentLoaderSource(
    source: Source,
    frameManager: FrameManager,
    startTS: Option[LocalDateTime] = Option.empty,
    endTSExcl: Option[LocalDateTime] = Option.empty
) {
  private val collectedIdsWithPlaceholders = mutable.ArrayBuffer[(Array[String], String)]()
  private var dependentSet: Boolean        = false

  /**
    * Adds a dependent DataFrame that should be used for loading the Table.
    * The distinct values in the DataFrame's [[columnName]] will get collected
    * and placed instead of the [[queryPlaceholder]] into the select query.
    * The function can be called several times with various DataFrames or columns.
    * For each of them there needs to be a separate placeholder.
    *
    * @param df               DataFrame to get the values from.
    * @param columnName       DataFrame's column.
    * @param queryPlaceholder The select query's placeholder where to put the values from the DataFrame.
    */
  def addDependentFrame(df: DataFrame, columnName: String, queryPlaceholder: String): Unit = {
    if (source.getSelectQuery == null) {
      throw new IllegalArgumentException("The source select query needs to be defined.")
    }

    if (!source.getSelectQuery.contains(queryPlaceholder)) {
      throw new IllegalArgumentException(s"The select query doesn't contain this placeholder: $queryPlaceholder")
    }

    if (!df.columns.contains(columnName)) {
      throw new IllegalArgumentException(s"The DataFrame doesn't contain this column: $columnName")
    }

    val collectedIds = df
      .select(columnName)
      .distinct
      .collect
      .map(row => row.get(0).toString)

    println("Number of collected ids: " + collectedIds.length)

    collectedIdsWithPlaceholders.append((collectedIds, queryPlaceholder))

    dependentSet = true
  }

  /**
    * Uses all the previously added dependent DataFrames to load the source (or simply loads it as the TableWrapper
    * would do if the function [[addDependentFrame()]] was never called.
    *
    * @param maxNumberOfIds         The maximum number of ids in one select query. If more values were collected,
    *                               the query will be split and executed several times. The results will then be unioned
    *                               into one DataFrame.
    * @param dataFrameReaderOptions Optional DataFrame reader options. Need to be valid SparkReader options.
    * @return Loaded source DataFrame.
    */
  def getFrame(maxNumberOfIds: Int = 1000000,
               dataFrameReaderOptions: Option[Map[String, String]] = Option.empty): DataFrame = {
    if (dependentSet) {
      val originalSelectQuery: String            = source.getSelectQuery
      val sorted: Array[(Array[String], String)] = collectedIdsWithPlaceholders.toArray.sortBy(x => x._1.length).reverse
      val grouped: List[Array[String]]           = sorted.head._1.grouped(maxNumberOfIds).toList
      val groupedPlaceholder: String             = sorted.head._2
      val dfs = grouped
        .map((largestIds: Array[String]) => {
          val filledSelectQuery =
            (Array((largestIds, groupedPlaceholder)) ++ sorted.tail).foldLeft(originalSelectQuery)(
              (query: String, ids: (Array[String], String)) => {
                println(s"placeholder: ${ids._2}, array size: ${ids._1.length}")
                val collectedString = "'" + ids._1.mkString("','") + "'"
                query.replace(ids._2, collectedString)
              }
            )

          source.setSelectQuery(filledSelectQuery)

          frameManager.loadSourceDataFrame(source, startTS, endTSExcl, dataFrameReaderOptions)
        })

      dfs.reduce((a, b) => a.union(b)).cache
    } else {
      frameManager.loadSourceDataFrame(source, startTS, endTSExcl, dataFrameReaderOptions)
    }
  }
}
