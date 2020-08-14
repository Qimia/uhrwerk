package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Config{
    private Connection[] connections;

    public Config(){}

    public Connection[] getConnections() {
        return connections;
    }

    public void setConnections(Connection[] connections) {
        this.connections = connections;
    }

    public void validate(String path){
        path+="config/";
        if(connections==null){
            throw new ConfigException("Missing field: " + path + "connections");
        }
        else {
            for(Connection c: connections){
                c.validate(path);
            }
        }
    }
}
