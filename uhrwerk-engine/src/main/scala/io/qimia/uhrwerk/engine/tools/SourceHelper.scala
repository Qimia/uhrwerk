package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.model.Source
import io.qimia.uhrwerk.engine.Environment.SourceIdent

object SourceHelper {
  def extractSourceIdent(source: Source): SourceIdent =
    SourceIdent(source.getConnection.getName, source.getPath, source.getFormat)
}
