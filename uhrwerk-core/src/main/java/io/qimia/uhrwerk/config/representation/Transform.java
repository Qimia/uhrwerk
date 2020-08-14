package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Transform extends Representation{

    private String type;
    private Partition partition;

    public Transform() {}

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    @Override
    public void validate(String path){
        path += "transform/";
        if(type == null){
            throw new ConfigException("Missing field: " + path + "type");
        }
        if(partition == null){
            throw new ConfigException("Missing field: " + path + "partition");
        }
        else{
            partition.validate(path);
        }
    }
}
