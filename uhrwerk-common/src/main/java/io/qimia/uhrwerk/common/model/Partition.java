package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;

public class Partition implements Comparable<Partition> {
    Long id;
    Long targetId;
    LocalDateTime partitionTs;
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
        return partitionSize == partition.partitionSize &&
                Objects.equals(id, partition.id) &&
                Objects.equals(targetId, partition.targetId) &&
                Objects.equals(partitionTs, partition.partitionTs) &&
                partitionUnit == partition.partitionUnit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, targetId, partitionTs, partitionUnit, partitionSize);
    }

    @Override
    public String toString() {
        return "Partition{" +
                "id=" + id +
                ", targetId=" + targetId +
                ", partitionTs=" + partitionTs +
                ", partitionUnit=" + partitionUnit +
                ", partitionSize=" + partitionSize +
                '}';
    }

    @Override
    public int compareTo(Partition o) {
        return this.getId().compareTo(o.getId());
    }
}
