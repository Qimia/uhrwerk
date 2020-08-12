package io.qimia.uhrwerk.config.representation;

public class Config {
    private Connection[] connections;

    public Config(){}

    public Connection[] getConnections() {
        return connections;
    }

    public void setConnections(Connection[] connections) {
        this.connections = connections;
    }
}
