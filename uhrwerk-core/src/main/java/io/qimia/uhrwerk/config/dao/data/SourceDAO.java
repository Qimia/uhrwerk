package io.qimia.uhrwerk.config.dao.data;

import io.qimia.uhrwerk.backend.model.data.Source;

public class SourceDAO {
    public static Source convertSourceToBackend(io.qimia.uhrwerk.config.model.Source source) {
        Source backendSource = new Source();
        backendSource.setPartitionColumn(source.getPartitionColumn());
        backendSource.setSqlPartitionQuery(source.getPartitionQuery());
        backendSource.setQueryColumn(source.getQueryColumn());
        backendSource.setSqlSelectQuery(source.getSelectQuery());

        return backendSource;
    }
}
