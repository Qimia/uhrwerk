package io.qimia.uhrwerk.models.store;

import javax.persistence.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

// Task logs for persistance
@Entity
@Table( name = "tasklog" )
public class TaskLog {

    private int id;
    private String stepName;
    // Either a stepConfig is given and there is a foreign-key. Or in devmode the configs are not persisted and
    // it will stay empty
    private StepConfig stepConfig;
    private int runNumber;
    private int version;
    private LocalDateTime runTs;
    private Duration runDuration;
    private int logType;

    public TaskLog() {}

    public TaskLog(
            String stepName,
            StepConfig stepConfig,
            int runNumber,
            int version,
            LocalDateTime runTs,
            Duration runDuration,
            int logType
    ) {
        this.stepName = stepName;
        this.stepConfig = stepConfig;
        this.runNumber = runNumber;
        this.version = version;
        this.runTs = runTs;
        this.runDuration = runDuration;
        this.logType = logType;
    }

    public TaskLog(
            String stepName,
            int runNumber,
            int version,
            LocalDateTime runTs,
            Duration runDuration,
            int logType
    ) {
        this.stepName = stepName;
        this.stepConfig = null;
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
    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    @ManyToOne
    @JoinColumn(name="stepConfigId")
    public StepConfig getStep() {
        return stepConfig;
    }
    public void setStep(StepConfig stepConfig) {
        this.stepConfig = stepConfig;
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
    public int getLogType() {
        return logType;
    }

    public void setLogType(int logType) {
        this.logType = logType;
    }
}
