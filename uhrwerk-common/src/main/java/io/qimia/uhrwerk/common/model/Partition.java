package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Partition {
  Long id;
  Long targetId;
  LocalDateTime partitionTs;
  String year;
  String month;
  String day;
  String hour;
  String minute;
  PartitionUnit partitionUnit;
  int partitionSize;

  public void setKey() {
    StringBuilder res =
        new StringBuilder().append(targetId).append(partitionTs.toEpochSecond(ZoneOffset.UTC));
    long id = LongHashFunction.xx().hashChars(res);
    this.setId(id);
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
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

  public String getYear() {
    return year;
  }

  public void setYear(String year) {
    this.year = year;
  }

  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
  }

  public String getDay() {
    return day;
  }

  public void setDay(String day) {
    this.day = day;
  }

  public String getHour() {
    return hour;
  }

  public void setHour(String hour) {
    this.hour = hour;
  }

  public String getMinute() {
    return minute;
  }

  public void setMinute(String minute) {
    this.minute = minute;
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Partition partition = (Partition) o;
    return partitionSize == partition.partitionSize
        && Objects.equals(id, partition.id)
        && Objects.equals(targetId, partition.targetId)
        && Objects.equals(partitionTs, partition.partitionTs)
        && Objects.equals(year, partition.year)
        && Objects.equals(month, partition.month)
        && Objects.equals(day, partition.day)
        && Objects.equals(hour, partition.hour)
        && Objects.equals(minute, partition.minute)
        && partitionUnit == partition.partitionUnit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, targetId, partitionTs, year, month, day, hour, minute, partitionUnit, partitionSize);
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
        + ", year='"
        + year
        + '\''
        + ", month='"
        + month
        + '\''
        + ", day='"
        + day
        + '\''
        + ", hour='"
        + hour
        + '\''
        + ", minute='"
        + minute
        + '\''
        + ", partitionUnit="
        + partitionUnit
        + ", partitionSize="
        + partitionSize
        + '}';
  }
}
