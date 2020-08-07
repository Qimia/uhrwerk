package io.qimia.uhrwerk.models.store;

import io.qimia.uhrwerk.models.TaskLogType;

import javax.persistence.*;
import java.time.Duration;
import java.time.LocalDateTime;

// Task logs for persistance
@Entity
@Table( name = "tasklog" )
public class TaskLog {

    private int id;
    private String tableName;
    // Either a stepConfig is given and there is a foreign-key. Or in devmode the configs are not persisted and
    // it will stay empty
    private TableConfig tableConfig;
    private int runNumber;
    private int version;
    private LocalDateTime runTs;
    private Duration runDuration;
    private TaskLogType logType;

    public TaskLog() {}

    public TaskLog(
            String tableName,
            TableConfig tableConfig,
            int runNumber,
            int version,
            LocalDateTime runTs,
            Duration runDuration,
            TaskLogType logType
    ) {
        this.tableName = tableName;
        this.tableConfig = tableConfig;
        this.runNumber = runNumber;
        this.version = version;
        this.runTs = runTs;
        this.runDuration = runDuration;
        this.logType = logType;
    }

    public TaskLog(
            String tableName,
            int runNumber,
            int version,
            LocalDateTime runTs,
            Duration runDuration,
            TaskLogType logType
    ) {
        this.tableName = tableName;
        this.tableConfig = null;  // Optional step reference
        this.runNumber = runNumber;
        this.version = version;
        this.runTs = runTs;
        this.runDuration = runDuration;
        this.logType = logType;
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
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @ManyToOne
    @JoinColumn(name="stepConfigId")
    public TableConfig getStep() {
        return tableConfig;
    }
    public void setStep(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    @Column
    public int getRunNumber() {
        return runNumber;
    }

    public void setRunNumber(int runNumber) {
        this.runNumber = runNumber;
    }

    @Column
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Column
    public LocalDateTime getRunTs() {
        return runTs;
    }

    public void setRunTs(LocalDateTime runTs) {
        this.runTs = runTs;
    }

    @Column
    public Duration getRunDuration() {
        return runDuration;
    }

    public void setRunDuration(Duration runDuration) {
        this.runDuration = runDuration;
    }

    @Column
    @Enumerated(EnumType.ORDINAL)
    public TaskLogType getLogType() {
        return logType;
    }

    public void setLogType(TaskLogType logType) {
        this.logType = logType;
    }
}
