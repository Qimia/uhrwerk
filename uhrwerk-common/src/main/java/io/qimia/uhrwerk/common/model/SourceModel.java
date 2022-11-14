package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;

public class SourceModel extends StateModel implements Comparable<SourceModel>, Serializable {
  private static final long serialVersionUID = 407099634243598968L;

  Long tableId;
  Long connectionId;
  String path;
  String format;
  ConnectionModel connection;
  TableModel table;
  PartitionUnit partitionUnit;
  int partitionSize;
  String parallelLoadQuery;
  String parallelLoadColumn;
  int parallelLoadNum;
  String selectQuery;
  String selectColumn;
  boolean partitioned = false;
  boolean autoLoad = true;

  public SourceModel(SourceModelBuilder builder) {
    super(builder.deactivatedTs);
    this.id = builder.id;
    this.tableId = builder.tableId;
    this.connectionId = builder.connectionId;
    this.path = builder.path;
    this.format = builder.format;
    this.connection = builder.connection;
    this.table = builder.table;
    this.partitionUnit = builder.partitionUnit;
    this.partitionSize = builder.partitionSize;
    this.parallelLoadQuery = builder.parallelLoadQuery;
    this.parallelLoadColumn = builder.parallelLoadColumn;
    this.parallelLoadNum = builder.parallelLoadNum;
    this.selectQuery = builder.selectQuery;
    this.selectColumn = builder.selectColumn;
    this.partitioned = builder.partitioned;
    this.autoLoad = builder.autoLoad;
  }

  public static SourceModelBuilder builder() {
    return new SourceModelBuilder();
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public Long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(Long connectionId) {
    this.connectionId = connectionId;
  }

  public ConnectionModel getConnection() {
    return connection;
  }

  public void setConnection(ConnectionModel connection) {
    this.connection = connection;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
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

  public String getParallelLoadQuery() {
    return parallelLoadQuery;
  }

  public void setParallelLoadQuery(String parallelLoadQuery) {
    this.parallelLoadQuery = parallelLoadQuery;
  }

  public String getParallelLoadColumn() {
    return parallelLoadColumn;
  }

  public void setParallelLoadColumn(String parallelLoadColumn) {
    this.parallelLoadColumn = parallelLoadColumn;
  }

  public int getParallelLoadNum() {
    return parallelLoadNum;
  }

  public void setParallelLoadNum(int parallelLoadNum) {
    this.parallelLoadNum = parallelLoadNum;
  }

  public String getSelectQuery() {
    return selectQuery;
  }

  public void setSelectQuery(String selectQuery) {
    this.selectQuery = selectQuery;
  }

  public String getSelectColumn() {
    return selectColumn;
  }

  public void setSelectColumn(String selectColumn) {
    this.selectColumn = selectColumn;
  }

  public boolean isPartitioned() {
    return partitioned;
  }

  public void setPartitioned(boolean partitioned) {
    this.partitioned = partitioned;
  }

  public boolean isAutoLoad() {
    return autoLoad;
  }

  public void setAutoLoad(boolean autoLoad) {
    this.autoLoad = autoLoad;
  }

  @Override
  public int compareTo(SourceModel o) {
    return this.getId().compareTo(o.getId());
  }

  public TableModel getTable() {
    return table;
  }

  public void setTable(TableModel table) {
    this.table = table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceModel)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SourceModel that = (SourceModel) o;
    return getPartitionSize() == that.getPartitionSize()
        && getParallelLoadNum() == that.getParallelLoadNum()
        && isPartitioned() == that.isPartitioned()
        && isAutoLoad() == that.isAutoLoad()
        && Objects.equal(getTableId(), that.getTableId())
        && Objects.equal(getConnectionId(), that.getConnectionId())
        && Objects.equal(getPath(), that.getPath())
        && Objects.equal(getFormat(), that.getFormat())
        && getPartitionUnit() == that.getPartitionUnit()
        && Objects.equal(getParallelLoadQuery(), that.getParallelLoadQuery())
        && Objects.equal(getParallelLoadColumn(), that.getParallelLoadColumn())
        && Objects.equal(getSelectQuery(), that.getSelectQuery())
        && Objects.equal(getSelectColumn(), that.getSelectColumn());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        super.hashCode(),
        getTableId(),
        getConnectionId(),
        getPath(),
        getFormat(),
        getPartitionUnit(),
        getPartitionSize(),
        getParallelLoadQuery(),
        getParallelLoadColumn(),
        getParallelLoadNum(),
        getSelectQuery(),
        getSelectColumn(),
        isPartitioned(),
        isAutoLoad());
  }

  @Override
  public String toString() {
    return "SourceModel{" +
        "tableId=" + tableId +
        ", connectionId=" + connectionId +
        ", path='" + path + '\'' +
        ", format='" + format + '\'' +
        ", partitionUnit=" + partitionUnit +
        ", partitionSize=" + partitionSize +
        ", parallelLoadQuery='" + parallelLoadQuery + '\'' +
        ", parallelLoadColumn='" + parallelLoadColumn + '\'' +
        ", parallelLoadNum=" + parallelLoadNum +
        ", selectQuery='" + selectQuery + '\'' +
        ", selectColumn='" + selectColumn + '\'' +
        ", partitioned=" + partitioned +
        ", autoLoad=" + autoLoad +
        '}';
  }
}
