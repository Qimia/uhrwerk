package io.qimia.uhrwerk.models.store;

import javax.persistence.*;
import java.time.Duration;

@Entity
@Table( name = "targetconfigs" )
public class TargetConfig {

    private int id;
    private TableInfo table;
    private Duration partitionSize;
    private TableConfig tableConfig;
    private String type;

    public TargetConfig() {}

    public TargetConfig(TableInfo table, Duration partitionSize, TableConfig tableConfig, String type) {
        this.table = table;
        this.partitionSize = partitionSize;
        this.tableConfig = tableConfig;
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

    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="tableInfoId")
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

    @ManyToOne(fetch=FetchType.LAZY)
    @JoinColumn(name="stepConfigId")
    public TableConfig getStepConfig() {
        return tableConfig;
    }

    public void setStepConfig(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    @Column
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
