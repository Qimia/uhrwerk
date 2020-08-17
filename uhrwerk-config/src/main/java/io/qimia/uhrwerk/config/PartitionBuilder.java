package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Partition;

public class PartitionBuilder {
    private String unit;
    private Integer size;

    public PartitionBuilder withUnit(String unit) {
        this.unit = unit;
        return this;
    }

    public PartitionBuilder withSize(Integer size) {
        this.size = size;
        return this;
    }

    public Partition build(){
        Partition partition = new Partition();
        partition.setSize(this.size);
        partition.setUnit(this.unit);
        return new Partition();
    }
}
