package io.qimia.uhrwerk.models.config;

import io.qimia.uhrwerk.models.DependencyType;
import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Dependency implements Table {

    private String connectionName = "";
    private String path = "";
    private String area = "";
    private String vertical = "";
    private int version = 1;
    private String type = "oneonone";
    private String partitionSize = "";
    private int partitionCount = 1;

    public Dependency() {}

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getType() {
        return type;
    }

    public DependencyType getTypeEnum() {
        return DependencyType.getDependencyType(type);
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPartitionSize() {
        return partitionSize;
    }

    public Duration getPartitionSizeDuration() {
        return TimeTools.convertDurationToObj(partitionSize);
    }

    public void setPartitionSize(String partitionSize) {
        this.partitionSize = partitionSize;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }
}
