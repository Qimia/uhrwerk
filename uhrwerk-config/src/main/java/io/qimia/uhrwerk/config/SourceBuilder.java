package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

public class SourceBuilder {
    private Source source;

    public SourceBuilder(Source source) {
        this.source = source;
    }


    public SourceBuilder connection(Connection connection){
        this.source.setConnection(connection);
        return this;
    }

    public SourceBuilder path(String path){
        this.source.setPath(path);
        return this;
    }

    public SourceBuilder format(String format){
        this.source.setFormat(format);
        return this;
    }

    public SourceBuilder partitionUnit(PartitionUnit partitionUnit){
        this.source.setPartitionUnit(partitionUnit);
        return this;
    }

    public SourceBuilder partitionSize(int partitionSize){
        this.source.setPartitionSize(partitionSize);
        return this;
    }

    public SourceBuilder parallelLoadQuery(String parallelLoadQuery){
        this.source.setParallelLoadQuery(parallelLoadQuery);
        return this;
    }

    public SourceBuilder parallelLoadColumn(String parallelLoadColumn){
        this.source.setParallelLoadColumn(parallelLoadColumn);
        return this;
    }

    public SourceBuilder parallelLoadNum(int parallelLoadNum){
        this.source.setParallelLoadNum(parallelLoadNum);
        return this;
    }

    public SourceBuilder selectQuery(String selectQuery){
        this.source.setSelectQuery(selectQuery);
        return this;
    }

    public SourceBuilder selectColumn(String selectColumn){
        this.source.setSelectColumn(selectColumn);
        return this;
    }

    public Source build(){
        return this.source;
    }
}
