package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Partition;
import io.qimia.uhrwerk.common.model.PartitionUnit;

import java.time.LocalDateTime;

public class PartitionBuilder {
    private Partition partition;

    public PartitionBuilder(Partition partition) {
        this.partition = partition;
    }


    public PartitionBuilder partitionTs(LocalDateTime partitionTs){
        this.partition.setPartitionTs(partitionTs);
        return this;
    }
    

    public PartitionBuilder partitionUnit(PartitionUnit partitionUnit){
        this.partition.setPartitionUnit(partitionUnit);
        return this;
    }

    public PartitionBuilder partitionSize(int partitionSize){
        this.partition.setPartitionSize(partitionSize);
        return this;
    }


    public Partition build(){
        return this.partition;
    }
}
