package io.qimia.uhrwerk.common.model;

import java.util.Objects;


public class Source {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Source source = (Source) o;
    return partitionSize == source.partitionSize &&
            parallelLoadNum == source.parallelLoadNum &&
            Objects.equals(connection, source.connection) &&
            Objects.equals(path, source.path) &&
            Objects.equals(format, source.format) &&
            partitionUnit == source.partitionUnit &&
            Objects.equals(parallelLoadQuery, source.parallelLoadQuery) &&
            Objects.equals(parallelLoadColumn, source.parallelLoadColumn) &&
            Objects.equals(selectQuery, source.selectQuery) &&
            Objects.equals(selectColumn, source.selectColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connection, path, format, partitionUnit, partitionSize, parallelLoadQuery, parallelLoadColumn, parallelLoadNum, selectQuery, selectColumn);
  }

  @Override
  public String toString() {
    return "Source{" +
            "connection=" + connection +
            ", path='" + path + '\'' +
            ", format='" + format + '\'' +
            ", partitionUnit=" + partitionUnit +
            ", partitionSize=" + partitionSize +
            ", parallelLoadQuery='" + parallelLoadQuery + '\'' +
            ", parallelLoadColumn='" + parallelLoadColumn + '\'' +
            ", parallelLoadNum=" + parallelLoadNum +
            ", selectQuery='" + selectQuery + '\'' +
            ", selectColumn='" + selectColumn + '\'' +
            '}';
  }
}
