package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class Table implements Serializable {

  private static final long serialVersionUID = 8806220232347910731L;

  Long id;
  String area;
  String vertical;
  String name;
  String version;
  String className;
  int parallelism;
  int maxBulkSize;
  PartitionUnit partitionUnit;
  int partitionSize;
  boolean partitioned = false;
  Dependency[] dependencies;
  Source[] sources;
  Target[] targets;

  public void setKey() {
    StringBuilder res =
        new StringBuilder()
            .append(this.getArea())
            .append(this.getVertical())
            .append(this.getName())
            .append(this.getVersion());
    long tableId = LongHashFunction.xx().hashChars(res);
    this.setId(tableId);
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

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public int getMaxBulkSize() {
    return maxBulkSize;
  }

  public void setMaxBulkSize(int maxBulkSize) {
    this.maxBulkSize = maxBulkSize;
  }

  public PartitionUnit getPartitionUnit() {
    return partitionUnit;
  }

  public void setPartitionUnit(PartitionUnit partitionUnit) {
    this.partitionUnit = partitionUnit;
  }

  public int getPartitionSize() {
    return partitionSize;
  }

  public void setPartitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
  }

  public Dependency[] getDependencies() {
    return dependencies;
  }

  public void setDependencies(Dependency[] dependencies) {
    this.dependencies = dependencies;
  }

  public Source[] getSources() {
    return sources;
  }

  public void setSources(Source[] sources) {
    this.sources = sources;
  }

  public Target[] getTargets() {
    return targets;
  }

  public void setTargets(Target[] targets) {
    this.targets = targets;
  }

  public boolean isPartitioned() {
    return partitioned;
  }

  public void setPartitioned(boolean partitioned) {
    this.partitioned = partitioned;
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Table table = (Table) o;
    if (dependencies != null) {
      Arrays.sort(dependencies);
    }
    if (table.dependencies != null) {
      Arrays.sort(table.dependencies);
    }
    Arrays.sort(targets);
    Arrays.sort(table.targets);
    if (sources != null) {
      Arrays.sort(sources);
    }
    if (table.sources != null) {
      Arrays.sort(table.sources);
    }

    return parallelism == table.parallelism
        && maxBulkSize == table.maxBulkSize
        && partitionSize == table.partitionSize
        && Objects.equals(area, table.area)
        && Objects.equals(vertical, table.vertical)
        && Objects.equals(name, table.name)
        && Objects.equals(version, table.version)
        && partitionUnit == table.partitionUnit
        && Arrays.equals(dependencies, table.dependencies)
        && Arrays.equals(sources, table.sources)
        && Arrays.equals(targets, table.targets);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            area, vertical, name, version, parallelism, maxBulkSize, partitionUnit, partitionSize);
    result = 31 * result + Arrays.hashCode(dependencies);
    result = 31 * result + Arrays.hashCode(sources);
    result = 31 * result + Arrays.hashCode(targets);
    return result;
  }

  @Override
  public String toString() {
    return "Table{"
        + "id='"
        + id
        + '\''
        + ", area='"
        + area
        + '\''
        + ", vertical='"
        + vertical
        + '\''
        + ", className='"
        + className
        + '\''
        + ", name='"
        + name
        + '\''
        + ", version='"
        + version
        + '\''
        + ", parallelism="
        + parallelism
        + ", maxBulkSize="
        + maxBulkSize
        + ", partitionUnit="
        + partitionUnit
        + ", partitionSize="
        + partitionSize
        + ", partitioned="
        + partitioned
        + ", dependencies="
        + Arrays.toString(dependencies)
        + ", sources="
        + Arrays.toString(sources)
        + ", targets="
        + Arrays.toString(targets)
        + '}';
  }
}
