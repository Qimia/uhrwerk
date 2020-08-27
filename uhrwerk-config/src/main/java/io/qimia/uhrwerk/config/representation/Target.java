package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

public class Target{

    private String connection_name;
    private String format;

    public Target() {}

    public String getConnection_name() {
        return connection_name;
    }

    public void setConnection_name(String connection_name) {
        this.connection_name = connection_name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void validate(String path){
        path += "target/";
        if(connection_name == null){
            throw new ConfigException("Missing field: " + path + "connection_name");
        }
        if(format == null){
            throw new ConfigException("Missing field: " + path + "fomart");
        }
    }

    @Override
    public String toString() {
        return "Target{" +
                "connection_name='" + connection_name + '\'' +
                ", format='" + format + '\'' +
                '}';
    }
}
