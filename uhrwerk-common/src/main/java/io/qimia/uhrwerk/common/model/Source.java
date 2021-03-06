package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.io.Serializable;
import java.util.Objects;

public class Source implements Comparable<Source>, Serializable {
  private static final long serialVersionUID = 407099634243598968L;

  Long id;
  Long tableId;
  Connection connection;
  String path;
  String format;
  PartitionUnit partitionUnit;
  int partitionSize;
  String parallelLoadQuery;
  String parallelLoadColumn;
  int parallelLoadNum;
  String selectQuery;
  String selectColumn;
  boolean partitioned = false;
  boolean autoloading = true;

  public boolean isAutoloading() {
    return autoloading;
  }

  public void setAutoloading(boolean autoloading) {
    this.autoloading = autoloading;
  }

  public void setKey() {
    StringBuilder res =
        new StringBuilder()
            .append(this.getConnection().getId())
            .append(this.getPath())
            .append(this.getFormat());
    long id = LongHashFunction.xx().hashChars(res);
    this.setId(id);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Source source = (Source) o;
    return partitionSize == source.partitionSize
        && parallelLoadNum == source.parallelLoadNum
        && partitioned == source.partitioned
        && Objects.equals(id, source.id)
        && Objects.equals(tableId, source.tableId)
        && Objects.equals(connection, source.connection)
        && Objects.equals(path, source.path)
        && Objects.equals(format, source.format)
        && partitionUnit == source.partitionUnit
        && Objects.equals(parallelLoadQuery, source.parallelLoadQuery)
        && Objects.equals(parallelLoadColumn, source.parallelLoadColumn)
        && Objects.equals(selectQuery, source.selectQuery)
        && Objects.equals(selectColumn, source.selectColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id,
        tableId,
        connection,
        path,
        format,
        partitionUnit,
        partitionSize,
        parallelLoadQuery,
        parallelLoadColumn,
        parallelLoadNum,
        selectQuery,
        selectColumn,
        partitioned,
        autoloading);
  }

  @Override
  public String toString() {
    return "Source{"
        + "id="
        + id
        + ", tableId="
        + tableId
        + ", connection="
        + connection
        + ", path='"
        + path
        + '\''
        + ", format='"
        + format
        + '\''
        + ", partitionUnit="
        + partitionUnit
        + ", partitionSize="
        + partitionSize
        + ", parallelLoadQuery='"
        + parallelLoadQuery
        + '\''
        + ", parallelLoadColumn='"
        + parallelLoadColumn
        + '\''
        + ", parallelLoadNum="
        + parallelLoadNum
        + ", selectQuery='"
        + selectQuery
        + '\''
        + ", selectColumn='"
        + selectColumn
        + '\''
        + ", partitioned="
        + partitioned
        + ", autoloading="
        + autoloading
        + '}';
  }

  @Override
  public int compareTo(Source o) {
    return this.getId().compareTo(o.getId());
  }
}
