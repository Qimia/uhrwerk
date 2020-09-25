package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent, TableIdent}
import io.qimia.uhrwerk.engine.{TableTransformation, TaskInput, TaskOutput}
import org.apache.spark.sql.functions.{col, hash}

class HashLoad extends TableTransformation {
  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   *
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  override def process(in: TaskInput): TaskOutput = {
    val dfIdent = in.loadedInputFrames.head._1
    val df = in.loadedInputFrames.head._2
    TaskOutput(df.withColumn(s"${getTableName(dfIdent)}", hash(df.columns.map(col): _*)))
  }

  def getTableName(i: Ident): String = {
    i match {
      case SourceIdent(connection, path, format) =>
        val parts = path.split(".")
        if (parts.length > 1) {
          parts(parts.length - 1)
        } else {
          path
        }
      case TableIdent(area, vertical, name, version) => name
    }
  }
}
