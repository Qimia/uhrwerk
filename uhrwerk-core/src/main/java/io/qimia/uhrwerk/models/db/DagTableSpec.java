package io.qimia.uhrwerk.models.db;

import java.sql.Timestamp;
import java.util.Objects;

public class DagTableSpec {
  Long id;
  String area;
  String vertical;
  String tableMame;
  String version;
  Timestamp createdTs;
  Timestamp updatedTs;
  Boolean external;
  String description;

  public DagTableSpec() {}

  public DagTableSpec(
      String area,
      String vertical,
      String tableMame,
      String version,
      Boolean external,
      String description) {
    this.area = area;
    this.vertical = vertical;
    this.tableMame = tableMame;
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

  public String getTableMame() {
    return tableMame;
  }

  public void setTableMame(String tableMame) {
    this.tableMame = tableMame;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DagTableSpec that = (DagTableSpec) o;
    return Objects.equals(id, that.id)
        && Objects.equals(area, that.area)
        && Objects.equals(vertical, that.vertical)
        && Objects.equals(tableMame, that.tableMame)
        && Objects.equals(version, that.version)
        && Objects.equals(createdTs, that.createdTs)
        && Objects.equals(updatedTs, that.updatedTs)
        && Objects.equals(external, that.external)
        && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, area, vertical, tableMame, version, createdTs, updatedTs, external, description);
  }

  @Override
  public String toString() {
    return "DagTableSpec{"
        + "id="
        + id
        + ", area='"
        + area
        + '\''
        + ", vertical='"
        + vertical
        + '\''
        + ", tableMame='"
        + tableMame
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
        + '}';
  }
}
