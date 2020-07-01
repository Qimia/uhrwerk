package io.qimia.uhrwerk.models.store;

import javax.persistence.*;
import java.time.Duration;
import java.time.LocalDateTime;

// Job logs for persistance
@Entity
@Table( name = "joblog" )
public class JobLog {

    private int id;
    private LocalDateTime runTs;
    private Duration runDuration;
    private int logType;
    private int[] taskIds;

    public JobLog(){}

    public JobLog(int id, LocalDateTime runTs, Duration runDuration, int logType, int[] taskIds) {
        this.id = id;
        this.runTs = runTs;
        this.runDuration = runDuration;
        this.logType = logType;
        this.taskIds = taskIds;
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
    public int getLogType() {
        return logType;
    }

    public void setLogType(int logType) {
        this.logType = logType;
    }

    @Column
    public int[] getTaskIds() {
        return taskIds;
    }

    public void setTaskIds(int[] taskIds) {
        this.taskIds = taskIds;
    }
}
