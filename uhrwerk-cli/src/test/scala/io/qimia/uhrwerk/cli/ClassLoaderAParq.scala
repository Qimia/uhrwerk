package io.qimia.uhrwerk.cli

import io.qimia.uhrwerk.engine.{TableTransformation, TaskInput, TaskOutput}

class ClassLoaderAParq extends TableTransformation {
  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   *
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  override def process(in: TaskInput): TaskOutput = TaskOutput(in.loadedInputFrames.head._2)
}
