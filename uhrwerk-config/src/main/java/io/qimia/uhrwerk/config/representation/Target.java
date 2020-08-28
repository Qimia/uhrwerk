package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

import java.util.Arrays;

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
            throw new ConfigException("Missing field: " + path + "format");
        }
        if (!Arrays.asList("json", "parquet", "jdbc", "orc", "libsvm", "csv", "text" , "avro").contains(format)) {
            throw new ConfigException("Wrong format! '" + format + "' is not allowed in " + path + "format");
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
