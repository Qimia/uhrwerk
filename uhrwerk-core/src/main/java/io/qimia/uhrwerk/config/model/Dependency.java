package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.config.DependencyType;
import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Dependency implements TableInput {

    private String connectionName = "";
    private String path = "";
    private String area = "";
    private String vertical = "";
    private int version = 1;
    private String type = "oneonone";
    private String partitionSize = "";
    private int partitionCount = 1;
    private String partitionQuery = "";
    private String partitionColumn = "";
    private String selectQuery = "";
    private String queryColumn = "";
    private int sparkReaderNumPartitions = 10;

    public Dependency() {}

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

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public DependencyType getTypeEnum() {
        return DependencyType.getDependencyType(type);
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPartitionSize() {
        return partitionSize;
    }

    public Duration getPartitionSizeDuration() {
        return TimeTools.convertDurationToObj(partitionSize);
    }

    public void setPartitionSize(String partitionSize) {
        this.partitionSize = partitionSize;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
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

    public int getSparkReaderNumPartitions() {
        return sparkReaderNumPartitions;
    }

    public void setSparkReaderNumPartitions(int sparkReaderNumPartitions) {
        this.sparkReaderNumPartitions = sparkReaderNumPartitions;
    }
}
