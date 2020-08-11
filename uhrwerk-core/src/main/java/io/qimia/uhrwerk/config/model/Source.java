package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.config.PartitionTemporalType;
import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

public class Source {

    private Long id;
    private Long cfTableId;
    private Long connectionId;

    private String connectionName = "";
    private String format = "";
    private String path = "";
    private String version = "1";
    private String partitionSize = "";
    private Integer partitionSizeInt = 0;
    private PartitionTemporalType partitionSizeType;
    private String partitionQuery = "";
    private String partitionColumn = "";
    private String selectQuery = "";
    private String queryColumn  = "";
    private Integer sparkReaderNumPartitions = 10;

    private LocalDateTime createdTS;
    private LocalDateTime updatedTS;

    public Source() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getCfTableId() {
        return cfTableId;
    }

    public void setCfTableId(Long cfTableId) {
        this.cfTableId = cfTableId;
    }

    public Long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Long connectionId) {
        this.connectionId = connectionId;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getPath() {
        return path;
    }

    public Optional<String> getOptionalPath() {
        return Optional.of(path);
    }

    public void setPath(String path) {
        this.path = path;
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

    public Integer getSparkReaderNumPartitions() {
        return sparkReaderNumPartitions;
    }

    public void setSparkReaderNumPartitions(Integer sparkReaderNumPartitions) {
        this.sparkReaderNumPartitions = sparkReaderNumPartitions;
    }

    public LocalDateTime getCreatedTS() {
        return createdTS;
    }

    public void setCreatedTS(LocalDateTime createdTS) {
        this.createdTS = createdTS;
    }

    public LocalDateTime getUpdatedTS() {
        return updatedTS;
    }

    public void setUpdatedTS(LocalDateTime updatedTS) {
        this.updatedTS = updatedTS;
    }
}
