package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Source implements StepInput {

    private String connectionName = "";
    private String path = "";
    private int version = 1;
    private String partitionSize = "";
    private String partitionQuery = "";
    private String partitionColumn = "";
    private String selectQuery = "";
    private String queryColumn  = "";

    public Source() {}

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getPartitionSize() {
        return partitionSize;
    }

    public void setPartitionSize(String partitionSize) {
        this.partitionSize = partitionSize;
    }

    public Duration getPartitionSizeDuration() {
        return TimeTools.convertDurationToObj(partitionSize);
    }

    public String getPartitionQuery() {
        return partitionQuery;
    }

    public void setPartitionQuery(String partitionQuery) {
        this.partitionQuery = partitionQuery;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public String getSelectQuery() {
        return selectQuery;
    }

    public void setSelectQuery(String selectQuery) {
        this.selectQuery = selectQuery;
    }

    public String getQueryColumn() {
        return queryColumn;
    }

    public void setQueryColumn(String queryColumn) {
        this.queryColumn = queryColumn;
    }
}