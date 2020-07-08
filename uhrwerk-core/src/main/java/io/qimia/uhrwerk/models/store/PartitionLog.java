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
    private ConnectionConfig connectionConfig;
    private String path;
    private LocalDateTime partitionTs;
    private Duration partitionDuration;
    private int version;
    private TaskLog task;
    private int changeFlag;

    public PartitionLog() {}

    public PartitionLog(
            String area,
            String vertical,
            ConnectionConfig connectionConfig,
            String path,
            LocalDateTime partitionTs,
            Duration partitionDuration,
            int version,
            TaskLog task,
            int changeFlag
    ) {
        this.area = area;
        this.vertical = vertical;
        this.connectionConfig = connectionConfig;
        this.path = path;
        this.partitionTs = partitionTs;
        this.partitionDuration = partitionDuration;
        this.version = version;
        this.task = task;
        this.changeFlag = changeFlag;
    }

    public PartitionLog(
            String area,
            String vertical,
            String path,
            LocalDateTime partitionTs,
            Duration partitionDuration,
            int version,
            TaskLog task,
            int changeFlag
    ) {
        this.area = area;
        this.vertical = vertical;
        this.connectionConfig = null;  // Optional reference to the configuration
        this.path = path;
        this.partitionTs = partitionTs;
        this.partitionDuration = partitionDuration;
        this.version = version;
        this.task = task;
        this.changeFlag = changeFlag;
        // TODO: Technically no room for testing which connection is used now
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

    @ManyToOne
    @JoinColumn(name="connectionConfigId")
    public ConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
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

    @ManyToOne
    @JoinColumn(name="taskLogId")
    public TaskLog getTask() {
        return task;
    }

    public void setTask(TaskLog task) {
        this.task = task;
    }

    @Column
    public int getChangeFlag() {
        return changeFlag;
    }

    public void setChangeFlag(int changeFlag) {
        this.changeFlag = changeFlag;
    }
}
