package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

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
    public void validate(String path){
        path+="config/";
        if(connections.length==0){
            throw new ConfigException("Missing field: " + path + "connections");
        }
        else {
            for(Connection c: connections){
                c.validate(path);
            }
        }
    }
}
