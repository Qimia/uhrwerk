package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import net.openhft.hashing.LongHashFunction;

public class Partition extends BaseModel implements Comparable<Partition>, Serializable {

  private static final long serialVersionUID = 2874825626697147072L;
  Long targetId;
  LocalDateTime partitionTs;
  PartitionUnit partitionUnit;
  int partitionSize;
  boolean partitioned;
  boolean bookmarked;

  String maxBookmark;

  public static PartitionBuilder builder() {
    return new PartitionBuilder();
  }

  public Partition(PartitionBuilder builder) {
    this.id = builder.id;
    this.targetId = builder.targetId;
    this.partitionTs = builder.partitionTs;
    this.partitionUnit = builder.partitionUnit;
    this.partitionSize = builder.partitionSize;
    this.partitioned = builder.partitioned;
    this.bookmarked = builder.bookmarked;
    this.maxBookmark = builder.maxBookmark;
  }

  public void setPartitioned(boolean partitioned) {
    this.partitioned = partitioned;
  }

  public void setKey() {
    StringBuilder res =
        new StringBuilder()
            .append(targetId)
            .append(partitionTs.toEpochSecond(ZoneOffset.UTC))
            .append(partitioned);
    long id = LongHashFunction.xx().hashChars(res);
    this.setId(id);
  }

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

  public LocalDateTime getPartitionTs() {
    return partitionTs;
  }

  public void setPartitionTs(LocalDateTime partitionTs) {
    this.partitionTs = partitionTs;
    ZonedDateTime partitionTsUTC = this.partitionTs.atZone(ZoneId.of("UTC"));
  }

  public boolean isPartitioned() {
    return partitioned;
  }

  public boolean isBookmarked() {
    return bookmarked;
  }

  public void setBookmarked(boolean bookmarked) {
    this.bookmarked = bookmarked;
  }

  public String getMaxBookmark() {
    return maxBookmark;
  }

  public void setMaxBookmark(String maxBookmark) {
    this.maxBookmark = maxBookmark;
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

  @Override
  public int compareTo(Partition o) {
    return this.getId().compareTo(o.getId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Partition)) {
      return false;
    }
    Partition partition = (Partition) o;
    return getPartitionSize() == partition.getPartitionSize()
        && isPartitioned() == partition.isPartitioned()
        && isBookmarked() == partition.isBookmarked()
        && Objects.equal(getTargetId(), partition.getTargetId())
        && Objects.equal(getPartitionTs(), partition.getPartitionTs())
        && getPartitionUnit() == partition.getPartitionUnit()
        && Objects.equal(getMaxBookmark(), partition.getMaxBookmark());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getTargetId(),
        getPartitionTs(),
        getPartitionUnit(),
        getPartitionSize(),
        isPartitioned(),
        isBookmarked(),
        getMaxBookmark());
  }

  @Override
  public String toString() {
    return "Partition{"
        + "id="
        + id
        + ", targetId="
        + targetId
        + ", partitionTs="
        + partitionTs
        + ", partitionUnit="
        + partitionUnit
        + ", partitionSize="
        + partitionSize
        + ", partitioned="
        + partitioned
        + ", bookmarked="
        + bookmarked
        + ", maxBookmark='"
        + maxBookmark
        + '\''
        + '}';
  }
}
