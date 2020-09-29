package io.qimia.uhrwerk.example.retail_examples

import io.qimia.uhrwerk.engine.Environment.SourceIdent
import io.qimia.uhrwerk.engine.{TableTransformation, TaskInput, TaskOutput}

class StagingSalesFact extends TableTransformation {
  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   *
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  override def process(in: TaskInput): TaskOutput = {
    in.loadedInputFrames.get(SourceIdent("retail_mysql", "qimia_oltp.sales_items", "jdbc")) match {
      case Some(x) => TaskOutput(x)
      case None => throw new Exception("Table sales not found!")
    }
  }
}
