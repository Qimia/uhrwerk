package io.qimia.uhrwerk.config.representation;

import java.util.ArrayList;

public class Config extends Representation{
    private Connection[] connections;

    public Config(){}

    public Connection[] getConnections() {
        return connections;
    }

    public void setConnections(Connection[] connections) {
        this.connections = connections;
    }

    @Override
    public void validate(){

    }
}
