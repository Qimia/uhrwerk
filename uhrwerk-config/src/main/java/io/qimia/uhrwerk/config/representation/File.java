package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class File{
    private String path;

    public File(){}

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public void validate(String path){
        if(this.path==null){
            throw new ConfigException("Missing field: " + path + "path");
        }
    }
}
