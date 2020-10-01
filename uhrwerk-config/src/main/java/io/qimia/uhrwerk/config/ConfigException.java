package io.qimia.uhrwerk.config;

public class ConfigException extends RuntimeException{
    private static final long serialVersionUID = -6773921536915313390L;

    public ConfigException(){}

    public ConfigException(String exception){
        super(exception);
    }

}
