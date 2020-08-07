package io.qimia.uhrwerk.backend.model.config;

import java.util.Objects;

public class Connection {

  private Long id;
  private String connectionName;
  private String connectionType;
  private String connectionUrl;
  private String version;
  private java.sql.Timestamp createdTs;
  private java.sql.Timestamp updatedTs;
  private String description;

  public Connection() {}

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  public String getConnectionType() {
    return connectionType;
  }

  public void setConnectionType(String connectionType) {
    this.connectionType = connectionType;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public java.sql.Timestamp getCreatedTs() {
    return createdTs;
  }

  public void setCreatedTs(java.sql.Timestamp createdTs) {
    this.createdTs = createdTs;
  }

  public java.sql.Timestamp getUpdatedTs() {
    return updatedTs;
  }

  public void setUpdatedTs(java.sql.Timestamp updatedTs) {
    this.updatedTs = updatedTs;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Connection that = (Connection) o;
    return Objects.equals(id, that.id) &&
            Objects.equals(connectionName, that.connectionName) &&
            Objects.equals(connectionType, that.connectionType) &&
            Objects.equals(connectionUrl, that.connectionUrl) &&
            Objects.equals(version, that.version) &&
            Objects.equals(createdTs, that.createdTs) &&
            Objects.equals(updatedTs, that.updatedTs) &&
            Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, connectionName, connectionType, connectionUrl, version, createdTs, updatedTs, description);
  }

  @Override
  public String toString() {
    return "ConfigConnection{" +
            "id=" + id +
            ", connectionName='" + connectionName + '\'' +
            ", connectionType='" + connectionType + '\'' +
            ", connectionUrl='" + connectionUrl + '\'' +
            ", version='" + version + '\'' +
            ", createdTs=" + createdTs +
            ", updatedTs=" + updatedTs +
            ", description='" + description + '\'' +
            '}';
  }
}
