package io.qimia.uhrwerk.config.model;

import io.qimia.uhrwerk.backend.model.BatchTemporalUnit;
import io.qimia.uhrwerk.config.PartitionTemporalType;
import io.qimia.uhrwerk.utils.TimeTools;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;

public class Table {

    private Long id;

    private String name;
    private int parallelism = 1;
    private int maxBatches = 0;
    private Dependency[] dependencies;
    private Source[] sources;
    private Target[] targets;
    private String area = "";
    private String vertical = "";
    private String partitionSize = "";
    private Integer partitionSizeInt = 0;
    private PartitionTemporalType partitionSizeType;
    private String version = "1";

    private LocalDateTime createdTS;
    private LocalDateTime updatedTS;

    public Table() {}

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getPartitionSize() {
        return partitionSize;
    }

    public Duration getPartitionSizeDuration() {
        return TimeTools.convertDurationToObj(partitionSize);
    }

    public void setPartitionSize(String partitionSize) {
        this.partitionSize = partitionSize;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public LocalDateTime getCreatedTS() {
        return createdTS;
    }

    public void setCreatedTS(LocalDateTime createdTS) {
        this.createdTS = createdTS;
    }

    public LocalDateTime getUpdatedTS() {
        return updatedTS;
    }

    public void setUpdatedTS(LocalDateTime updatedTS) {
        this.updatedTS = updatedTS;
    }

    public String getPath() {
        return Paths.get("area=", area,
                "vertical=", vertical,
                "table=", name,
                "version=", version).toString();
    }
}
