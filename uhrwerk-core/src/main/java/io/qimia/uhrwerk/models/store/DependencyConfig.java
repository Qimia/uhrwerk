package io.qimia.uhrwerk.models.store;

import javax.persistence.*;
import java.time.Duration;

@Entity
@Table(name = "dependencyconfigs")
public class DependencyConfig {

    private int id;
    private TableInfo table;
    private Duration partitionSize;
    private int partitionCount;
    private StepConfig stepConfig;
    private String type;

    public DependencyConfig() {
    }

    public DependencyConfig(
            TableInfo table,
            Duration partitionSize,
            int partitionCount,
            StepConfig stepConfig,
            String type
    ) {
        this.table = table;
        this.partitionSize = partitionSize;
        this.partitionCount = partitionCount;
        this.stepConfig = stepConfig;
        this.type = type;
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

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "tableInfoId")
    public TableInfo getTable() {
        return table;
    }

    public void setTable(TableInfo table) {
        this.table = table;
    }

    @Column
    public Duration getPartitionSize() {
        return partitionSize;
    }

    public void setPartitionSize(Duration partitionSize) {
        this.partitionSize = partitionSize;
    }

    @Column
    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "stepConfigId")
    public StepConfig getStepConfig() {
        return stepConfig;
    }

    public void setStepConfig(StepConfig stepConfig) {
        this.stepConfig = stepConfig;
    }

    @Column
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
