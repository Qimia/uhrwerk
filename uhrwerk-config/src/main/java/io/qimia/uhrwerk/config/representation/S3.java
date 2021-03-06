package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class S3{
    private String path;
    private String secret_id;
    private String secret_key;

    public S3(){}

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getSecret_id() {
        return secret_id;
    }

    public void setSecret_id(String secret_id) {
        this.secret_id = secret_id;
    }

    public String getSecret_key() {
        return secret_key;
    }

    public void setSecret_key(String secret_key) {
        this.secret_key = secret_key;
    }

    public void validate(String path){
        path += "s3/";
        if(this.path == null){
            throw new ConfigException("Missing field: " + path + "path");
        }
        if(secret_id == null){
            throw new ConfigException("Missing field: " + path + "secret_id");
        }
        if(secret_key == null){
            throw new ConfigException("Missing field: " + path + "secret_key");
        }
    }


    @Override
    public String toString() {
        return "S3{" +
                "path='" + path + '\'' +
                ", secret_id='" + secret_id + '\'' +
                ", secret_key='" + secret_key + '\'' +
                '}';
    }
}
