package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

import java.lang.reflect.Field;

public class Representation {
    public void validate(String path){
        path += getClass().getName() + "/";
        for (Field f: getClass().getDeclaredFields()){
            try {
                if(f.get(this)==null){
                    throw new ConfigException("Missing field: " + path + f.getName());
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
