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
        Target target = new Target();
        target.setConnection_name(this.connectionName);
        target.setFormat(this.format);
        return target;
    }
}
