package io.qimia.uhrwerk.models.config;

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
