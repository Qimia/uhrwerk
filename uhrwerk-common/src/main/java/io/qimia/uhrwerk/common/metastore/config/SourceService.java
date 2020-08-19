package io.qimia.uhrwerk.common.metastore.config;

import io.qimia.uhrwerk.common.model.Source;

public interface SourceService {
    SourceResult save(Source source, boolean overwrite);
}
