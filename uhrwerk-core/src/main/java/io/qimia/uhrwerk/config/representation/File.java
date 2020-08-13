package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class File extends Representation{
    private String path;

    public File(){}

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public void validate(String path){
        if(path==null){
            throw new ConfigException("Missing field: " + path + "path");
        }
    }
}
