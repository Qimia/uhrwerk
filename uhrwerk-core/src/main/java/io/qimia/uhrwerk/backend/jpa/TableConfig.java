package io.qimia.uhrwerk.backend.jpa;

import javax.persistence.*;
import java.time.Duration;

@Entity
@Table( name = "stepconfigs" )
public class TableConfig {

    private int id;
    private String name;
    private Duration batchSize;
    private int parallelism;
    private int maxBatches;

    public TableConfig() {}

    public TableConfig(String name, Duration batchSize, int parallelism, int maxBatches) {
        this.name = name;
        this.batchSize = batchSize;
        this.parallelism = parallelism;
        this.maxBatches = maxBatches;
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
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column
    public Duration getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Duration batchSize) {
        this.batchSize = batchSize;
    }

    @Column
    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    @Column
    public int getMaxBatches() {
        return maxBatches;
    }

    public void setMaxBatches(int maxBatches) {
        this.maxBatches = maxBatches;
    }

}
