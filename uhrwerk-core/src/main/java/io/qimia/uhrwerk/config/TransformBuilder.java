package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Transform;

public class TransformBuilder {
    private String type;
    private Partition partition;

    public TransformBuilder withType(String type) {
        this.type = type;
        return this;
    }

    public TransformBuilder withPartition(Partition partition) {
        this.partition = partition;
        return this;
    }

    public Transform build(){
        return new Transform();
    }
}
