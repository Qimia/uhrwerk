package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.utils.TimeTools;

import java.time.Duration;

public class Table {

    private String name;
    private String batchSize;
    private int parallelism = 1;
    private int maxBatches = 0;
    private Dependency[] dependencies;
    private Source[] sources;
    private Target[] targets;
    private String targetArea = "";
    private String targetVertical = "";
    private String targetPartitionSize = "";
    private int targetVersion = 1;

    public Table() {}

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


    public boolean dependenciesSet() {
        return dependencies != null;
    }

    public Dependency[] getDependencies() {
        return dependencies;
    }

    public void setDependencies(Dependency[] dependencies) {
        this.dependencies = dependencies;
    }

    public boolean sourcesSet() {
        return sources != null;
    }

    public Source[] getSources() {
        return sources;
    }

    public void setSources(Source[] sources) {
        this.sources = sources;
    }

    public Target[] getTargets() {
        return targets;
    }

    public void setTargets(Target[] targets) {
        this.targets = targets;
    }

    public String getTargetArea() {
        return targetArea;
    }

    public void setTargetArea(String targetArea) {
        this.targetArea = targetArea;
    }

    public String getTargetVertical() {
        return targetVertical;
    }

    public void setTargetVertical(String targetVertical) {
        this.targetVertical = targetVertical;
    }

    public String getTargetPartitionSize() {
        return targetPartitionSize;
    }

    public Duration getTargetPartitionSizeDuration() {
        return TimeTools.convertDurationToObj(targetPartitionSize);
    }

    public void setTargetPartitionSize(String targetPartitionSize) {
        this.targetPartitionSize = targetPartitionSize;
    }

    public int getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(int targetVersion) {
        this.targetVersion = targetVersion;
    }
}
