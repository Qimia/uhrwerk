package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Target;

public class TargetBuilder {
    private String connectionName;
    private String format;

    public TargetBuilder withConnectionName(String connectionName) {
        this.connectionName = connectionName;
        return this;
    }

    public TargetBuilder withFormat(String format) {
        this.format = format;
        return this;
    }

    public Target build(){
        return new Target();
    }
}
