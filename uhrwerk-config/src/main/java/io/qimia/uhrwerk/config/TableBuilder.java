package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.*;

import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.config.representation.*;
import io.qimia.uhrwerk.config.representation.Dependency;
import io.qimia.uhrwerk.config.representation.Partition;
import io.qimia.uhrwerk.config.representation.Source;
import io.qimia.uhrwerk.config.representation.Target;

import java.util.ArrayList;

public class TableBuilder {
    private io.qimia.uhrwerk.config.representation.Table table;
    private io.qimia.uhrwerk.config.representation.Partition partition;
    private io.qimia.uhrwerk.config.representation.Partition source_partition;
    private io.qimia.uhrwerk.config.representation.Partition dependency_partition;
    private ParallelLoad parallel_load;
    private Select select;
    private Transform transform;
    private ArrayList<io.qimia.uhrwerk.config.representation.Source> sourcesList;
    private ArrayList<io.qimia.uhrwerk.config.representation.Dependency> dependenciesList;
    private ArrayList<io.qimia.uhrwerk.config.representation.Target> targetsList;
    private io.qimia.uhrwerk.config.representation.Source[] sources;
    private io.qimia.uhrwerk.config.representation.Dependency[] dependencies;
    private io.qimia.uhrwerk.config.representation.Target[] targets;


    public TableBuilder() {
        this.table = new io.qimia.uhrwerk.config.representation.Table();
    }

    public TableBuilder area(String area) {
        if (this.table.getArea() == null) {
            this.table.setArea(area);
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getArea() == null) {
                this.dependenciesList.get(dependenciesList.size() - 1).setArea(area);
            }
        }
        return this;
    }

    public TableBuilder vertical(String vertical) {
        if (this.table.getVertical() == null) {
            this.table.setVertical(vertical);
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getVertical() == null) {
                this.dependenciesList.get(dependenciesList.size() - 1).setVertical(vertical);
            }
        }
        return this;
    }

    public TableBuilder table(String table) {
        if (this.table.getTable() == null) {
            this.table.setTable(table);
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTable() == null) {
                this.dependenciesList.get(dependenciesList.size() - 1).setTable(table);
            }
        }
        return this;
    }

    public TableBuilder version(String version) {
        if (this.table.getVersion() == null) {
            this.table.setVersion(version);
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getVersion() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setVersion(version);
            }
        }

        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getVersion() == null) {
                this.dependenciesList.get(dependenciesList.size() - 1).setVersion(version);
            }
        }
        return this;
    }

    public TableBuilder parallelism(int parallelism) {
        this.table.setParallelism(parallelism);
        return this;
    }

    public TableBuilder maxBulkSize(int maxBulkSize) {
        this.table.setMax_bulk_size(maxBulkSize);
        return this;
    }

    public TableBuilder partition(Partition partition) {
        if (this.table.getPartition() == null) {
            this.table.setPartition(partition);
        }
        if (this.sourcesList != null){
            if (this.sourcesList.get(sourcesList.size() - 1).getPartition() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setPartition(partition);
        }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null){
                if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition() == null) {
                    this.dependenciesList.get(dependenciesList.size() - 1).getTransform().setPartition(partition);
                }
            }
        }
        return this;
    }

    public TableBuilder partition() {
        if (this.table.getPartition() == null) {
            this.partition = new Partition();
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getPartition() == null) {
                this.source_partition = new Partition();
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null) {
                if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition() == null) {
                    this.dependency_partition = new Partition();
                }
            }
        }
        return this;
    }

    public TableBuilder unit(String unit) {
        if (this.partition == null && this.source_partition == null && this.dependency_partition == null){
            System.out.println("There is no partition defined to which a partition unit can be set!");
        }
        if (this.partition != null) {
            if (this.partition.getUnit() == null) {
                this.partition.setUnit(unit);
                this.table.setPartition(this.partition);
            }
        }
        if (this.source_partition != null) {
            if (this.source_partition.getUnit() == null) {
                this.source_partition.setUnit(unit);
                this.sourcesList.get(sourcesList.size() - 1).setPartition(this.source_partition);
            }
        }
        if (this.dependency_partition != null) {
            if (this.dependency_partition.getUnit() == null) {
                this.dependency_partition.setUnit(unit);
                this.dependenciesList.get(dependenciesList.size() - 1).getTransform().setPartition(this.dependency_partition);
            }
        }
        return this;
    }

    public TableBuilder size(int size) {
        if (this.partition == null && this.source_partition == null && this.dependency_partition == null){
            System.out.println("There is no partition defined to which a partition size can be set!");
        }
        if (this.partition != null) {
            if (this.partition.getSize() == null) {
                this.partition.setSize(size);
                this.table.setPartition(this.partition);
            }
        }
        if (this.source_partition != null) {
            if (this.source_partition.getSize() == null) {
                this.source_partition.setSize(size);
                this.sourcesList.get(sourcesList.size() - 1).setPartition(this.source_partition);
            }
        }
            if (this.dependency_partition != null) {
                if (this.dependency_partition.getSize() == null) {
                    this.dependency_partition.setSize(size);
                    this.dependenciesList.get(dependenciesList.size() - 1).getTransform().setPartition(this.dependency_partition);
                }
            }
        return this;
    }

    public TableBuilder sources(Source[] sources) {
        this.table.setSources(sources);
        return this;
    }

    public TableBuilder source() {
        if (this.sourcesList == null) {
            this.sourcesList = new ArrayList<Source>();
        }
        Source source = new Source();
        this.sourcesList.add(source);
        return this;
    }

    public TableBuilder targets(Target[] targets) {
        this.table.setTargets(targets);
        return this;
    }

    public TableBuilder target() {
        if (this.targetsList == null) {
            this.targetsList = new ArrayList<Target>();
        }
        Target target = new Target();
        this.targetsList.add(target);
        return this;
    }

    public TableBuilder connection_name(String connection_name) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getConnection_name() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setConnection_name(connection_name);
            }
        }
        if (this.targetsList != null) {
            if (this.targetsList.get(targetsList.size() - 1).getConnection_name() == null) {
                this.targetsList.get(targetsList.size() - 1).setConnection_name(connection_name);
            }
        }
        return this;
    }

    public TableBuilder path(String path) {
        this.sourcesList.get(sourcesList.size() - 1).setPath(path);
        return this;
    }

    public TableBuilder format(String format) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getFormat() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setFormat(format);
            }
        }
        if (this.targetsList != null) {
            if (this.targetsList.get(targetsList.size() - 1).getFormat() == null) {
                this.targetsList.get(targetsList.size() - 1).setFormat(format);
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getFormat() == null) {
                this.dependenciesList.get(dependenciesList.size() - 1).setFormat(format);
            }
        }
        return this;
    }


    public TableBuilder parallel_load(ParallelLoad parallel_load) {
        this.sourcesList.get(sourcesList.size() - 1).setParallel_load(parallel_load);
        return this;
    }

    public TableBuilder parallel_load() {
        this.parallel_load = new ParallelLoad();
        return this;
    }


    public TableBuilder query(String query) {
        if (this.parallel_load == null && this.select == null){
            System.out.println("There is no parallel_load or select defined to which a query can be set!");
        }
        if (this.parallel_load != null) {
            if (this.parallel_load.getQuery() == null) {
                this.parallel_load.setQuery(query);
                this.sourcesList.get(sourcesList.size() - 1).setParallel_load(this.parallel_load);
            }
        }
        if (this.select != null) {
            if (this.select.getQuery() == null) {
                this.select.setQuery(query);
                this.sourcesList.get(sourcesList.size() - 1).setSelect(this.select);
            }
        }
        return this;
    }

    public TableBuilder column(String column) {
        if (this.parallel_load == null && this.select == null){
            System.out.println("There is no parallel_load or select defined to which a column can be set!");
        }
        if (this.parallel_load != null) {
            if (this.parallel_load.getColumn() == null) {
                this.parallel_load.setColumn(column);
                this.sourcesList.get(sourcesList.size() - 1).setParallel_load(this.parallel_load);
            }
        }
        if (this.select != null) {
            if (this.select.getColumn() == null) {
                this.select.setColumn(column);
                this.sourcesList.get(sourcesList.size() - 1).setSelect(this.select);
            }
        }
        return this;
    }

    public TableBuilder num(int num) {
        this.parallel_load.setNum(num);
        this.sourcesList.get(sourcesList.size() - 1).setParallel_load(this.parallel_load);
        return this;
    }

    public TableBuilder select(Select select) {
        this.sourcesList.get(sourcesList.size() - 1).setSelect(select);
        return this;
    }

    public TableBuilder select() {
        this.select = new Select();
        return this;
    }

    public TableBuilder dependencies(io.qimia.uhrwerk.config.representation.Dependency[] dependencies) {
        this.table.setDependencies(dependencies);
        return this;
    }

    public TableBuilder dependency() {
        if (this.dependenciesList == null) {
            this.dependenciesList = new ArrayList<io.qimia.uhrwerk.config.representation.Dependency>();
        }
        io.qimia.uhrwerk.config.representation.Dependency dependency = new io.qimia.uhrwerk.config.representation.Dependency();
        this.dependenciesList.add(dependency);
        return this;
    }

    public TableBuilder transform(Transform transform) {
        this.dependenciesList.get(dependenciesList.size() - 1).setTransform(transform);
        return this;
    }

    public TableBuilder transform() {
        this.transform = new Transform();
        return this;
    }

    public TableBuilder type(String type) {
        this.transform.setType(type);
        this.dependenciesList.get(dependenciesList.size() - 1).setTransform(this.transform);
        return this;
    }

    public Table build() {
        if (this.sourcesList != null) {
            this.sources = new Source[sourcesList.size()];
            sourcesList.toArray(this.sources);
            this.table.setSources(this.sources);
        }
        if (this.dependenciesList != null) {
            this.dependencies = new Dependency[dependenciesList.size()];
            dependenciesList.toArray(this.dependencies);
            this.table.setDependencies(this.dependencies);
        }
        if (this.targetsList != null) {
            this.targets = new Target[targetsList.size()];
            targetsList.toArray(this.targets);
            this.table.setTargets(this.targets);
        }
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelTable(this.table);
    }
}
