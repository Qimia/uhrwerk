package io.qimia.uhrwerk.config.representation;


import io.qimia.uhrwerk.config.ConfigException;

public class Partition{

    private String unit;
    private Integer size;

    public Partition() {}

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public void validate(String path, String type){
        path += "partition/";
        if(type=="temporal_aggregate"){
            if(unit == null){
                throw new ConfigException("Missing field: " + path + "unit");
            }
        }
        if(size == 0){
            throw new ConfigException("Missing field: " + path + "size");
        }
    }
}
