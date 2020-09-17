package io.qimia.uhrwerk.engine

import io.qimia.uhrwerk.engine.Environment.{Ident, SourceIdent}
import org.apache.spark.sql.DataFrame

case class TaskInput(
                      loadedInputFrames: Map[Ident, DataFrame],
                      notLoadedSources: Map[SourceIdent, DependentLoaderSource]
                    )
