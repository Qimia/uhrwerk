package io.qimia.uhrwerk.engine

/**
 * User's need to implement this trait for any transformations
 */
trait TableTransformation {

  /**
   * Code for the transformation required to produce a particular Table
   * (UserCode)
   * @param in TaskInput, all data needed by a user
   * @return TaskOutput which is a dataframe and info about writing the dataframe
   */
  def process(in: TaskInput): TaskOutput

}
