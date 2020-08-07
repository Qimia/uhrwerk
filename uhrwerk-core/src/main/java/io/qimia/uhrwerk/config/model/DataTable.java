package io.qimia.uhrwerk.config.model;

import java.time.Duration;

public interface DataTable {

    public String getConnectionName();
    public void setConnectionName(String connectionName);
    public String getPath();
    public void setPath(String path);
    public String getArea();
    public void setArea(String area);
    public String getVertical();
    public void setVertical(String vertical);
    public int getVersion();
    public void setVersion(int version);
    public String getPartitionSize();
    public Duration getPartitionSizeDuration();
    public void setPartitionSize(String partitionSize);

}
