package io.qimia.uhrwerk.config.model;

import java.time.LocalDateTime;

public class Target {

  private long id;
  private long tableId;
  private long connectionId;

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

  public long getTableId() {
    return tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
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
