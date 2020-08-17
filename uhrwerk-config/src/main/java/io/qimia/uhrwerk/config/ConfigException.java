package io.qimia.uhrwerk.config;

public class ConfigException extends RuntimeException{
    public ConfigException(){}

    public ConfigException(String exception){
        super(exception);
    }

}
