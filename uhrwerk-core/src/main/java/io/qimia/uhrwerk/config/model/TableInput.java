package io.qimia.uhrwerk.config.model;

import java.time.Duration;

public interface TableInput {

    public String getConnectionName();
    public void setConnectionName(String connectionName);
    public String getPath();
    public void setPath(String path);
    public int getVersion();
    public void setVersion(int version);
    public String getPartitionSize();
    public Duration getPartitionSizeDuration();
    public void setPartitionSize(String partitionSize);
    public String getPartitionQuery();
    public void setPartitionQuery(String partitionQuery);
    public String getPartitionColumn();
    public void setPartitionColumn(String partitionColumn);
    public String getSelectQuery();
    public void setSelectQuery(String selectQuery);
    public String getQueryColumn();
    public void setQueryColumn(String queryColumn);
    public int getSparkReaderNumPartitions();
    public void setSparkReaderNumPartitions(int sparkReaderNumPartitions);
}
