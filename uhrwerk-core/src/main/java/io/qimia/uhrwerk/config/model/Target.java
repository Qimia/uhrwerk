package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Target {

    private String connectionName = "";
    private String path = "";

    public Target() {}

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
