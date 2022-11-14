package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.model.SourceModel
import io.qimia.uhrwerk.engine.Environment.SourceIdent

object SourceHelper {
  def extractSourceIdent(source: SourceModel): SourceIdent =
    SourceIdent(source.getConnection.getName, source.getPath, source.getFormat)
}
