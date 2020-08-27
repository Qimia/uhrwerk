package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Table;
import io.qimia.uhrwerk.config.representation.*;

import java.util.ArrayList;

public class OldTableBuilder {
    private io.qimia.uhrwerk.config.representation.Table table;
    private ArrayList<Source> sourcesList;
    private ArrayList<Dependency> dependenciesList;
    private ArrayList<Target> targetsList;
    private Source[] sources;
    private Dependency[] dependencies;
    private Target[] targets;


    public OldTableBuilder() {
        this.table = new io.qimia.uhrwerk.config.representation.Table();
    }


    public OldTableBuilder sources(Source[] sources) {
        this.table.setSources(sources);
        return this;
    }

    public OldTableBuilder source() {
        if (this.sourcesList == null) {
            this.sourcesList = new ArrayList<Source>();
        }
        Source source = new Source();
        this.sourcesList.add(source);
        return this;
    }

    public OldTableBuilder targets(Target[] targets) {
        this.table.setTargets(targets);
        return this;
    }

    public OldTableBuilder target() {
        if (this.targetsList == null) {
            this.targetsList = new ArrayList<Target>();
        }
        Target target = new Target();
        this.targetsList.add(target);
        return this;
    }

    public OldTableBuilder dependencies(Dependency[] dependencies) {
        this.table.setDependencies(dependencies);
        return this;
    }

    public OldTableBuilder dependency() {
        if (this.dependenciesList == null) {
            this.dependenciesList = new ArrayList<Dependency>();
        }
        Dependency dependency = new Dependency();
        this.dependenciesList.add(dependency);
        return this;
    }


    public OldTableBuilder area(String area) {
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

    public OldTableBuilder vertical(String vertical) {
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

    public OldTableBuilder table(String table) {
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

    public OldTableBuilder version(String version) {
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

    public OldTableBuilder parallelism(int parallelism) {
        this.table.setParallelism(parallelism);
        return this;
    }

    public OldTableBuilder maxBulkSize(int maxBulkSize) {
        this.table.setMax_bulk_size(maxBulkSize);
        return this;
    }


    public OldTableBuilder partition(Partition partition) {
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

    public OldTableBuilder partition() {
        if (this.table.getPartition() == null) {
            this.table.setPartition(new Partition());
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getPartition() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setPartition(new Partition());
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null) {
                if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition() == null) {
                    this.dependenciesList.get(dependenciesList.size() - 1).getTransform().setPartition(new Partition());
                }
            }
        }
        return this;
    }

    public OldTableBuilder unit(String unit) {
        if (this.table.getPartition() != null) {
            if (this.table.getPartition().getUnit() == null) {
                this.table.getPartition().setUnit(unit);
            }
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getPartition() != null) {
                if (this.sourcesList.get(sourcesList.size() - 1).getPartition().getUnit() == null) {
                    this.sourcesList.get(sourcesList.size() - 1).getPartition().setUnit(unit);
                }
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null) {
                if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition() != null) {
                    if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition().getUnit() == null) {
                        this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition().setUnit(unit);
                    }
                }
            }
        }
        return this;
    }

    public OldTableBuilder size(int size) {
        if (this.table.getPartition() != null) {
            if (this.table.getPartition().getSize() == null) {
                this.table.getPartition().setSize(size);
            }
        }
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getPartition() != null) {
                if (this.sourcesList.get(sourcesList.size() - 1).getPartition().getSize() == null) {
                    this.sourcesList.get(sourcesList.size() - 1).getPartition().setSize(size);
                }
            }
        }
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null) {
                if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition() != null) {
                    if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition().getSize() == null) {
                        this.dependenciesList.get(dependenciesList.size() - 1).getTransform().getPartition().setSize(size);
                    }
                }
            }
        }
        return this;
    }



    public OldTableBuilder connectionName(String connectionName) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size() - 1).getConnection_name() == null) {
                this.sourcesList.get(sourcesList.size() - 1).setConnection_name(connectionName);
            }
        }
        if (this.targetsList != null) {
            if (this.targetsList.get(targetsList.size() - 1).getConnection_name() == null) {
                this.targetsList.get(targetsList.size() - 1).setConnection_name(connectionName);
            }
        }
        return this;
    }

    public OldTableBuilder path(String path) {
        if (this.sourcesList != null) {
            this.sourcesList.get(sourcesList.size() - 1).setPath(path);
        }
        return this;
    }

    public OldTableBuilder format(String format) {
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


    public OldTableBuilder parallelLoad(ParallelLoad parallelLoad) {
        if (this.sourcesList != null) {
            this.sourcesList.get(sourcesList.size() - 1).setParallel_load(parallelLoad);
        }
        return this;
    }

    public OldTableBuilder parallelLoad() {
        if (this.sourcesList != null) {
            this.sourcesList.get(sourcesList.size() - 1).setParallel_load(new ParallelLoad());
        }
        return this;
    }


    public OldTableBuilder query(String query) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size()-1).getParallel_load() != null) {
                if (this.sourcesList.get(sourcesList.size()-1).getParallel_load().getQuery() == null) {
                    this.sourcesList.get(sourcesList.size()-1).getParallel_load().setQuery(query);
                }
            }
            if (this.sourcesList.get(sourcesList.size()-1).getSelect() != null) {
                if (this.sourcesList.get(sourcesList.size()-1).getSelect().getQuery() == null) {
                    this.sourcesList.get(sourcesList.size()-1).getSelect().setQuery(query);
                }
            }
        }
        return this;
    }

    public OldTableBuilder column(String column) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size()-1).getParallel_load() != null) {
                if (this.sourcesList.get(sourcesList.size()-1).getParallel_load().getColumn() == null) {
                    this.sourcesList.get(sourcesList.size()-1).getParallel_load().setColumn(column);
                }
            }
            if (this.sourcesList.get(sourcesList.size()-1).getSelect() != null) {
                if (this.sourcesList.get(sourcesList.size()-1).getSelect().getColumn() == null) {
                    this.sourcesList.get(sourcesList.size()-1).getSelect().setColumn(column);
                }
            }
        }
        return this;
    }

    public OldTableBuilder num(int num) {
        if (this.sourcesList != null) {
            if (this.sourcesList.get(sourcesList.size()-1).getParallel_load() != null) {
                if (this.sourcesList.get(sourcesList.size()-1).getParallel_load().getNum() == null) {
                    this.sourcesList.get(sourcesList.size()-1).getParallel_load().setNum(num);
                }
            }
        }
        return this;
    }

    public OldTableBuilder select(Select select) {
        if (this.sourcesList != null) {
            this.sourcesList.get(sourcesList.size() - 1).setSelect(select);
        }
        return this;
    }

    public OldTableBuilder select() {
        if (this.sourcesList != null) {
            this.sourcesList.get(sourcesList.size() - 1).setSelect(new Select());
        }
        return this;
    }


    public OldTableBuilder transform(Transform transform) {
        if (this.dependenciesList != null) {
            this.dependenciesList.get(dependenciesList.size() - 1).setTransform(transform);
        }
        return this;
    }

    public OldTableBuilder transform() {
        if (this.dependenciesList != null) {
            this.dependenciesList.get(dependenciesList.size() - 1).setTransform(new Transform());
        }
        return this;
    }

    public OldTableBuilder type(String type) {
        if (this.dependenciesList != null) {
            if (this.dependenciesList.get(dependenciesList.size() - 1).getTransform() != null) {
                this.dependenciesList.get(dependenciesList.size() - 1).getTransform().setType(type);
            }
        }
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
