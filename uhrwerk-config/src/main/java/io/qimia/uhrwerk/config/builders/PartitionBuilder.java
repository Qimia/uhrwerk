package io.qimia.uhrwerk.config.builders;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.PartitionUnit;

public class PartitionBuilder<P extends PartitionBuilder<P>> {

  private String unit;
  private Integer size;

  public PartitionBuilder() {}

  public P unit(String unit) {
    this.unit = unit;
    return (P) this;
  }

  public P size(Integer size) {
    this.size = size;
    return (P) this;
  }

  protected Partition build() {
    var partition = new Partition();
    partition.setSize(size);
    if (unit != null) partition.setUnit(PartitionUnit.valueOf(unit.toUpperCase()));
    return partition;
  }
}
