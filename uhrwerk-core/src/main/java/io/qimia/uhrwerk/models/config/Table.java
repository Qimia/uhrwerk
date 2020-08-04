package io.qimia.uhrwerk.models.config;

import java.time.Duration;

public interface Table {

    public String getConnectionName();
    public String getPath();
    public String getArea();
    public String getVertical();
    public int getVersion();
    public String getPartitionSize();
    public Duration getPartitionSizeDuration();

}
