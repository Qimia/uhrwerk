package io.qimia.uhrwerk.models.config;

public interface Table {

    public String getConnectionName();
    public String getPath();
    public String getArea();
    public String getVertical();
    public int getVersion();
    public boolean isExternal();
    public String getPartitionSize();

}
