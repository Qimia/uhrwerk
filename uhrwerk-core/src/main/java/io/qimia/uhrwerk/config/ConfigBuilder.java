package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.config.representation.Config;
import io.qimia.uhrwerk.config.representation.Connection;

public class ConfigBuilder {
    private Connection[] connections;

    public ConfigBuilder withConnections(Connection[] connections) {
        this.connections = connections;
        return this;
    }

    public Config build(){
        return new Config();
    }
}
