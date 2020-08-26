package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Partition;

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
    partition.setUnit(unit);
    partition.validate("");
    return partition;
  }
}
