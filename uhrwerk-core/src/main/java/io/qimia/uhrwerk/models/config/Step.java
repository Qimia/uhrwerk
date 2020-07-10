package io.qimia.uhrwerk.models.config;

import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Step {

    private String name;
    private String batchSize;
    private int parallelism;
    private int maxBatches;
    private String stepType = "oneOnone";
    private Dependency[] dependencies;
    private Target[] targets;
    private int version = 1;

    public Step() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBatchSize() {
        return batchSize;
    }

    public Duration getBatchSizeDuration() {
        return TimeTools.convertDurationToObj(batchSize);
    }

    public void setBatchSize(String batchSize) {
        this.batchSize = batchSize;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getMaxBatches() {
        return maxBatches;
    }

    public void setMaxBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

    public String getStepType() {
        return stepType;
    }

    public void setStepType(String stepType) {
        this.stepType = stepType;
    }

    public Dependency[] getDependencies() {
        return dependencies;
    }

    public void setDependencies(Dependency[] dependencies) {
        this.dependencies = dependencies;
    }

    public Target[] getTargets() {
        return targets;
    }

    public void setTargets(Target[] targets) {
        this.targets = targets;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
