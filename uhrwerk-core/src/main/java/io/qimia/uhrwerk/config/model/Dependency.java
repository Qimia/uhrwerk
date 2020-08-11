package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.config.DependencyType;
import io.qimia.uhrwerk.config.PartitionTemporalType;
import io.qimia.uhrwerk.utils.TimeTools;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;

public class Dependency {

  private Long id;
  private Long tableId;
  private Long targetId;

  private String connectionName = "";
  private String tableName = "";
  private String format = "";
  private String area = "";
  private String vertical = "";
  private String version = "1";
  private String type = "oneonone";
  private String partitionSize = "";  // Leave out of yaml-pojo
  private Integer partitionSizeInt = 0;
  private PartitionTemporalType partitionSizeType;
  private Integer partitionCount = 1;
  private String partitionQuery = "";
  private String partitionColumn = "";
  private String selectQuery = "";
  private String queryColumn = "";
  private Integer sparkReaderNumPartitions = 10;

  private LocalDateTime createdTS;
  private LocalDateTime updatedTS;

  public Dependency() {}

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

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

  public String getConnectionName() {
    return connectionName;
  }

  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
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

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getType() {
    return type;
  }

  public DependencyType getTypeEnum() {
    return DependencyType.getDependencyType(type);
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getPartitionSize() {
    return partitionSize;
  }

  public Duration getPartitionSizeDuration() {
    return TimeTools.convertDurationStrToObj(partitionSize);
  }

  public BatchTemporalUnit getBatchTemporalUnit() {
    return TimeTools.convertDurationToBatchTemporalUnit(getPartitionSizeDuration()).getOrElse(null);
  }

  public void setPartitionSize(String partitionSize) {
    this.partitionSize = partitionSize;
  // Set extra fields for persistence
        if (!partitionSize.equals("")) {
            var tuple = TimeTools.convertDurationStrToTuple(partitionSize);
            this.partitionSizeInt = tuple.count();
            this.partitionSizeType = tuple.durationUnit();
        }
    }

    public Integer getPartitionSizeInt() {
        return partitionSizeInt;
    }

    public PartitionTemporalType getPartitionSizeType() {
        return partitionSizeType;
    }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(Integer partitionCount) {
    this.partitionCount = partitionCount;
  }

  public String getPartitionQuery() {
    return partitionQuery;
  }

  public void setPartitionQuery(String partitionQuery) {
    this.partitionQuery = partitionQuery;
  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public void setPartitionColumn(String partitionColumn) {
    this.partitionColumn = partitionColumn;
  }

  public String getSelectQuery() {
    return selectQuery;
  }

  public void setSelectQuery(String selectQuery) {
    this.selectQuery = selectQuery;
  }

  public String getQueryColumn() {
    return queryColumn;
  }

  public void setQueryColumn(String queryColumn) {
    this.queryColumn = queryColumn;
  }

  public Integer getSparkReaderNumPartitions() {
    return sparkReaderNumPartitions;
  }

  public void setSparkReaderNumPartitions(Integer sparkReaderNumPartitions) {
    this.sparkReaderNumPartitions = sparkReaderNumPartitions;
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

  /**
   * Concatenates area, vertical, table, version, and format into a path. Either with slashes for a
   * file system or with dashes and a dot for jdbc.
   *
   * @param fileSystem Whether the path is for a file system or for jdbc.
   * @return The concatenated path.
   */
  public String getPath(Boolean fileSystem) {
    if (fileSystem) {
      return Paths.get(
              "area=",
              area,
              "vertical=",
              vertical,
              "table=",
              tableName,
              "version=",
              version,
              "format=",
              format)
          .toString();
    } else { // jdbc
      return area + "-" + vertical + "." + tableName + "-" + version;
    }
  }

  public void setPartitionSizeInt(Integer partitionSizeInt) {
    this.partitionSizeInt = partitionSizeInt;
  }

  public void setPartitionSizeType(PartitionTemporalType partitionSizeType) {
    this.partitionSizeType = partitionSizeType;
  }
}
