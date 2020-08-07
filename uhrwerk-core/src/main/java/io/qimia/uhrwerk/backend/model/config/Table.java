package io.qimia.uhrwerk.backend.model.config;

import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.backend.model.data.Dependency;
import io.qimia.uhrwerk.backend.model.data.Source;
import io.qimia.uhrwerk.backend.model.data.Target;

import java.util.List;
import java.util.Objects;

public class Table {

  private Long id;
  private String area;
  private String vertical;
  private String tableName;
  private BatchTemporalUnit batchTemporalUnit;
  private Integer batchSize;
  private Integer parallelism;
  private Integer maxPartitions;
  private String version;
  private java.sql.Timestamp createdTs;
  private java.sql.Timestamp updatedTs;
  private String description;

  private List<Target> targets;
  private List<Dependency> dependencies;
  private List<Source> sources;

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

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public BatchTemporalUnit getBatchTemporalUnit() {
    return batchTemporalUnit;
  }

  public void setBatchTemporalUnit(BatchTemporalUnit batchTemporalUnit) {
    this.batchTemporalUnit = batchTemporalUnit;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public Integer getMaxPartitions() {
    return maxPartitions;
  }

  public void setMaxPartitions(Integer maxPartitions) {
    this.maxPartitions = maxPartitions;
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
    Table table = (Table) o;
    return Objects.equals(id, table.id) &&
            Objects.equals(area, table.area) &&
            Objects.equals(vertical, table.vertical) &&
            Objects.equals(tableName, table.tableName) &&
            batchTemporalUnit == table.batchTemporalUnit &&
            Objects.equals(batchSize, table.batchSize) &&
            Objects.equals(parallelism, table.parallelism) &&
            Objects.equals(maxPartitions, table.maxPartitions) &&
            Objects.equals(version, table.version) &&
            Objects.equals(createdTs, table.createdTs) &&
            Objects.equals(updatedTs, table.updatedTs) &&
            Objects.equals(description, table.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, area, vertical, tableName, batchTemporalUnit, batchSize, parallelism, maxPartitions, version, createdTs, updatedTs, description);
  }

  @Override
  public String toString() {
    return "ConfigTable{" +
            "id=" + id +
            ", area='" + area + '\'' +
            ", vertical='" + vertical + '\'' +
            ", tableName='" + tableName + '\'' +
            ", batchTemporalUnit=" + batchTemporalUnit +
            ", batchSize=" + batchSize +
            ", parallelism=" + parallelism +
            ", maxPartitions=" + maxPartitions +
            ", version='" + version + '\'' +
            ", createdTs=" + createdTs +
            ", updatedTs=" + updatedTs +
            ", description='" + description + '\'' +
            '}';
  }

  public List<Target> getTargets() {
    return targets;
  }

  public void setTargets(List<Target> targets) {
    this.targets = targets;
  }

  public List<Dependency> getDependencies() {
    return dependencies;
  }

  public void setDependencies(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }

  public List<Source> getSources() {
    return sources;
  }

  public void setSources(List<Source> sources) {
    this.sources = sources;
  }
}
