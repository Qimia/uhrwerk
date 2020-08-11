package io.qimia.uhrwerk.config.model;

import java.time.LocalDateTime;

public class Target {

    private long id;
    private long cfTableId;
    private long cfConnectionId;

    private String connectionName = "";
    private String format = "";

    private LocalDateTime createdTS;
    private LocalDateTime updatedTS;

    public Target() {}

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getCfTableId() {
        return cfTableId;
    }

    public void setCfTableId(long cfTableId) {
        this.cfTableId = cfTableId;
    }

    public long getCfConnectionId() {
        return cfConnectionId;
    }

    public void setCfConnectionId(long cfConnectionId) {
        this.cfConnectionId = cfConnectionId;
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
