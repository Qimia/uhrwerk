package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Target;
import io.qimia.uhrwerk.common.model.Connection;

public class TargetBuilder {
    private Target target;

    public TargetBuilder() { this.target = new Target(); }

    public TargetBuilder connection(Connection connection) {
        this.target.setConnection(connection);
        return this;
    }

    public TargetBuilder format(String format) {
        this.target.setFormat(format);
        return this;
    }


    public Target build() {
        //this.target.validate("");
        return this.target;
    }
}
