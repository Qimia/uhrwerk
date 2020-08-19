package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

public class DependencyBuilder {
    private Dependency dependency;


    public DependencyBuilder(Dependency dependency) {
        this.dependency = dependency;
    }


    public DependencyBuilder area(String area){
        this.dependency.setArea(area);
        return this;
    }

    public DependencyBuilder vertical(String vertical){
        this.dependency.setVertical(vertical);
        return this;
    }

    public DependencyBuilder tableName(String tableName){
        this.dependency.setTableName(tableName);
        return this;
    }

    public DependencyBuilder format(String format){
        this.dependency.setFormat(format);
        return this;
    }

    public DependencyBuilder version(String version){
        this.dependency.setVersion(version);
        return this;
    }

    public DependencyBuilder transformType(PartitionTransformType transformType){
        this.dependency.setTransformType(transformType);
        return this;
    }

    public DependencyBuilder transformPartitionUnit(PartitionUnit transformPartitionUnit){
        this.dependency.setTransformPartitionUnit(transformPartitionUnit);
        return this;
    }

    public DependencyBuilder transformPartitionSize(int transformPartitionSize){
        this.dependency.setTransformPartitionSize(transformPartitionSize);
        return this;
    }

    public Dependency build(){
        return this.dependency;
    }
}
