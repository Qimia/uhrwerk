package io.qimia.uhrwerk.models.store;

import javax.persistence.*;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * data partitions logs for persistence
 * (ie. which partitions are stored in the datalake)
 */
@Entity
@Table( name = "partitionlog" )
public class PartitionLog {

    private int id;
    private String area;
    private String vertical;
    private int connectionId;
    private String path;
    private LocalDateTime partitionTs;
    private Duration partitionDuration;
    private int version;
    private int taskId;
    private int changeFlag;

    public PartitionLog() {}

    public PartitionLog(
            String area,
            String vertical,
            int connectionId,
            String path,
            LocalDateTime partitionTs,
            Duration partitionDuration,
            int version,
            int taskId,
            int changeFlag
    ) {
        this.area = area;
        this.vertical = vertical;
        this.connectionId = connectionId;
        this.path = path;
        this.partitionTs = partitionTs;
        this.partitionDuration = partitionDuration;
        this.version = version;
        this.taskId = taskId;
        this.changeFlag = changeFlag;
    }


    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id", updatable = false, nullable = false)
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Column
    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    @Column
    public String getVertical() {
        return vertical;
    }

    public void setVertical(String vertical) {
        this.vertical = vertical;
    }

    @Column
    public int getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(int connectionId) {
        this.connectionId = connectionId;
    }

    @Column
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Column
    public LocalDateTime getPartitionTs() {
        return partitionTs;
    }

    public void setPartitionTs(LocalDateTime partitionTs) {
        this.partitionTs = partitionTs;
    }

    @Column
    public Duration getPartitionDuration() {
        return partitionDuration;
    }

    public void setPartitionDuration(Duration partitionDuration) {
        this.partitionDuration = partitionDuration;
    }

    @Column
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Column
    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    @Column
    public int getChangeFlag() {
        return changeFlag;
    }

    public void setChangeFlag(int changeFlag) {
        this.changeFlag = changeFlag;
    }
}
