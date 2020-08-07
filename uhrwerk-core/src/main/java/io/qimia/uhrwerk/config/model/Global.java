package io.qimia.uhrwerk.config.model;

public class Global {
    private Connection[] connections = {};

    public Global() {}

    public Connection[] getConnections() {
        return connections;
    }

    public void setConnections(Connection[] connections) {
        this.connections = connections;
    }
}
