package io.qimia.uhrwerk.engine.tools

import io.qimia.uhrwerk.common.metastore.model.SourceModel2
import io.qimia.uhrwerk.engine.Environment.SourceIdent

object SourceHelper {
  def extractSourceIdent(source: SourceModel2): SourceIdent =
    SourceIdent(source.getConnection.getName, source.getPath, source.getFormat)
}
