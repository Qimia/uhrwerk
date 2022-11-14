package io.qimia.uhrwerk.common.model;

import java.time.LocalDateTime;

public class PartitionBuilder {

  Long id;
  Long targetId;
  LocalDateTime partitionTs;
  PartitionUnit partitionUnit;
  int partitionSize;
  boolean partitioned;
  boolean bookmarked;
  String maxBookmark;

  public PartitionBuilder id(Long id) {
    this.id = id;
    return this;
  }

  public PartitionBuilder targetId(Long targetId) {
    this.targetId = targetId;
    return this;
  }

  public PartitionBuilder partitionTs(LocalDateTime partitionTs) {
    this.partitionTs = partitionTs;
    return this;
  }

  public PartitionBuilder partitionUnit(PartitionUnit partitionUnit) {
    this.partitionUnit = partitionUnit;
    return this;
  }

  public PartitionBuilder partitionSize(int partitionSize) {
    this.partitionSize = partitionSize;
    return this;
  }

  public PartitionBuilder partitioned(boolean partitioned) {
    this.partitioned = partitioned;
    return this;
  }

  public PartitionBuilder bookmarked(boolean bookmarked) {
    this.bookmarked = bookmarked;
    return this;
  }

  public PartitionBuilder maxBookmark(String maxBookmark) {
    this.maxBookmark = maxBookmark;
    return this;
  }

  public Partition build() {
    return new Partition(this);
  }
}