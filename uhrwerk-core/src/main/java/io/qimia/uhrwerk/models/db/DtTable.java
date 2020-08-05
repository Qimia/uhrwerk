package io.qimia.uhrwerk.models.db;

import java.sql.Timestamp;
import java.util.Objects;

public class DtTable {
  Long id;
  Long tableSpecId;
  Long connectionId;
  String path;
  String version;
  Timestamp createdTs;
  Timestamp updatedTs;
  Boolean external;
  String description;
  DagTableSpec tableSpec;
  DagConnection connection;

  public DtTable() {}

  public DtTable(
      Long tableSpecId,
      Long connectionId,
      String path,
      String version,
      Boolean external,
      String description) {
    this.tableSpecId = tableSpecId;
    this.connectionId = connectionId;
    this.path = path;
    this.version = version;
    this.external = external;
    this.description = description;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getTableSpecId() {
    return tableSpecId;
  }

  public void setTableSpecId(Long tableSpecId) {
    this.tableSpecId = tableSpecId;
  }

  public Long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(Long connectionId) {
    this.connectionId = connectionId;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public Timestamp getCreatedTs() {
    return createdTs;
  }

  public void setCreatedTs(Timestamp createdTs) {
    this.createdTs = createdTs;
  }

  public Timestamp getUpdatedTs() {
    return updatedTs;
  }

  public void setUpdatedTs(Timestamp updatedTs) {
    this.updatedTs = updatedTs;
  }

  public Boolean getExternal() {
    return external;
  }

  public void setExternal(Boolean external) {
    this.external = external;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public DagTableSpec getTableSpec() {
    return tableSpec;
  }

  public void setTableSpec(DagTableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  public DagConnection getConnection() {
    return connection;
  }

  public void setConnection(DagConnection connection) {
    this.connection = connection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DtTable dtTable = (DtTable) o;
    return Objects.equals(id, dtTable.id)
        && Objects.equals(tableSpecId, dtTable.tableSpecId)
        && Objects.equals(connectionId, dtTable.connectionId)
        && Objects.equals(path, dtTable.path)
        && Objects.equals(version, dtTable.version)
        && Objects.equals(createdTs, dtTable.createdTs)
        && Objects.equals(updatedTs, dtTable.updatedTs)
        && Objects.equals(external, dtTable.external)
        && Objects.equals(description, dtTable.description)
        && Objects.equals(tableSpec, dtTable.tableSpec)
        && Objects.equals(connection, dtTable.connection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        tableSpecId,
        connectionId,
        path,
        version,
        createdTs,
        updatedTs,
        external,
        description,
        tableSpec,
        connection);
  }

  @Override
  public String toString() {
    return "DtTable{"
        + "id="
        + id
        + ", tableSpecId="
        + tableSpecId
        + ", connectionId="
        + connectionId
        + ", path='"
        + path
        + '\''
        + ", version='"
        + version
        + '\''
        + ", createdTs="
        + createdTs
        + ", updatedTs="
        + updatedTs
        + ", external="
        + external
        + ", description='"
        + description
        + '\''
        + ", tableSpec="
        + tableSpec
        + ", connection="
        + connection
        + '}';
  }
}
