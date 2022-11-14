package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.Arrays;

public class TableModel extends StateModel implements Serializable {

  private static final long serialVersionUID = 8806220232347910731L;

  final String area;
  final String vertical;
  final String name;
  final String version;
  String className;
  int parallelism;
  int maxBulkSize;
  PartitionUnit partitionUnit;
  int partitionSize;
  boolean partitioned = false;

  DependencyModel[] dependencies;
  SourceModel[] sources;
  TargetModel[] targets;

  public static TableModelBuilder builder() {
    return new TableModelBuilder();
  }

  public TableModel(TableModelBuilder builder) {
    super(builder.deactivatedTs);
    this.id = builder.id;
    this.area = builder.area;
    this.vertical = builder.vertical;
    this.name = builder.name;
    this.version = builder.version;
    this.className = builder.className;
    this.parallelism = builder.parallelism;
    this.maxBulkSize = builder.maxBulkSize;
    this.partitionUnit = builder.partitionUnit;
    this.partitionSize = builder.partitionSize;
    this.partitioned = builder.partitioned;
    this.dependencies = builder.dependencies;
    this.sources = builder.sources;
    this.targets = builder.targets;
  }
  public String getArea() {
    return area;
  }

  public String getVertical() {
    return vertical;
  }

  public String getName() {
    return name;
  }

  public String getVersion() {
    return version;
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

  public DependencyModel[] getDependencies() {
    return dependencies;
  }

  public void setDependencies(DependencyModel[] dependencies) {
    this.dependencies = dependencies;
  }

  public SourceModel[] getSources() {
    return sources;
  }

  public void setSources(SourceModel[] sources) {
    this.sources = sources;
  }

  public TargetModel[] getTargets() {
    return targets;
  }

  public void setTargets(TargetModel[] targets) {
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
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableModel)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    TableModel table = (TableModel) o;
    return getParallelism() == table.getParallelism()
        && getMaxBulkSize() == table.getMaxBulkSize()
        && getPartitionSize() == table.getPartitionSize()
        && isPartitioned() == table.isPartitioned()
        && Objects.equal(getArea(), table.getArea())
        && Objects.equal(getVertical(), table.getVertical())
        && Objects.equal(getName(), table.getName())
        && Objects.equal(getVersion(), table.getVersion())
        && Objects.equal(getClassName(), table.getClassName())
        && getPartitionUnit() == table.getPartitionUnit()
        && Objects.equal(getDependencies(), table.getDependencies())
        && Objects.equal(getSources(), table.getSources())
        && Objects.equal(getTargets(), table.getTargets());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(),
        getArea(),
        getVertical(),
        getName(),
        getVersion(),
        getClassName(),
        getParallelism(),
        getMaxBulkSize(),
        getPartitionUnit(),
        getPartitionSize(),
        isPartitioned(),
        getDependencies(),
        getSources(),
        getTargets());
  }

  @Override
  public String toString() {
    return "TableModel{" +
        "area='" + area + '\'' +
        ", vertical='" + vertical + '\'' +
        ", name='" + name + '\'' +
        ", version='" + version + '\'' +
        ", className='" + className + '\'' +
        ", parallelism=" + parallelism +
        ", maxBulkSize=" + maxBulkSize +
        ", partitionUnit=" + partitionUnit +
        ", partitionSize=" + partitionSize +
        ", partitioned=" + partitioned +
        ", dependencies=" + Arrays.toString(dependencies) +
        ", sources=" + Arrays.toString(sources) +
        ", targets=" + Arrays.toString(targets) +
        '}';
  }
}
