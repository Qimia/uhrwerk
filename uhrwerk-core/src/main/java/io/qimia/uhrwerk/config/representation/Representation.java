package io.qimia.uhrwerk.config.representation;

import java.lang.reflect.Field;

public class Representation {
    public ValidationResult validate(){
        Boolean valid = true;
        String fieldName = "";
        for (Field f: getClass().getDeclaredFields()){
            try {
                if(f.get(this)==null){
                    valid = false;
                    fieldName = f.getName();
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        ValidationResult v = new ValidationResult();
        v.valid=valid;
        v.fieldName=fieldName;
        return v;
    }
}
