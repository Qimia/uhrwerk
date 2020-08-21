package io.qimia.uhrwerk.config;

import io.qimia.uhrwerk.common.model.Dag;
import io.qimia.uhrwerk.config.representation.*;

import java.util.ArrayList;

public class DagBuilder {
    private io.qimia.uhrwerk.config.representation.Dag dag;
    private ArrayList<Table> tablesList;
    private ArrayList<Connection> connectionsList;
    private ArrayList<ArrayList<Source>> sourcesList;
    private ArrayList<ArrayList<Dependency>> dependenciesList;
    private ArrayList<ArrayList<Target>> targetsList;
    private Connection[] connections;
    private Table[] tables;

    public DagBuilder() {
        this.dag = new io.qimia.uhrwerk.config.representation.Dag();
        this.tablesList = new ArrayList<Table>();
        this.connectionsList = new ArrayList<Connection>();
    }


    public DagBuilder connections(Connection[] connections) {
        this.dag.setConnections(connections);
        return this;
    }

    public DagBuilder connection() {
        Connection connection = new Connection();
        this.connectionsList.add(connection);
        return this;
    }

    public DagBuilder tables(Table[] tables) {
        this.dag.setTables(tables);
        return this;
    }

    public DagBuilder table() {
        Table table = new Table();
        ArrayList<Source> sources = new ArrayList<Source>();
        ArrayList<Target> targets = new ArrayList<Target>();
        ArrayList<Dependency> dependencies = new ArrayList<Dependency>();
        if (this.sourcesList == null) {
            this.sourcesList = new ArrayList<ArrayList<Source>>();
        }
        if (this.targetsList == null) {
            this.targetsList = new ArrayList<ArrayList<Target>>();
        }
        if (this.dependenciesList == null) {
            this.dependenciesList = new ArrayList<ArrayList<Dependency>>();
        }
        this.tablesList.add(table);
        this.sourcesList.add(sources);
        this.targetsList.add(targets);
        this.dependenciesList.add(dependencies);
        return this;
    }


    public DagBuilder name(String name) {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setName(name);
        }
        return this;
    }

    public DagBuilder jdbc(JDBC jdbc) {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setJdbc(jdbc);
        }
        return this;
    }

    public DagBuilder jdbc() {
        if (this.connectionsList.size() != 0) {

            this.connectionsList.get(this.connectionsList.size() - 1).setJdbc(new JDBC());
        }
        return this;
    }

    public DagBuilder jdbcUrl(String url) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_url(url);
            }
        }
        return this;
    }

    public DagBuilder jdbcDriver(String driver) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setJdbc_driver(driver);
            }
        }
        return this;
    }

    public DagBuilder user(String user) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setUser(user);
            }
        }
        return this;
    }

    public DagBuilder pass(String pass) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getJdbc() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getJdbc().setPass(pass);
            }
        }
        return this;
    }

    public DagBuilder s3(S3 s3) {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setS3(s3);
        }
        return this;
    }

    public DagBuilder s3() {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setS3(new S3());
        }
        return this;
    }



    public DagBuilder secretId(String secretId) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_id(secretId);
            }
        }
        return this;
    }

    public DagBuilder secretKey(String secretKey) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null){
                this.connectionsList.get(this.connectionsList.size()-1).getS3().setSecret_key(secretKey);
            }
        }
        return this;
    }


    public DagBuilder file(File file) {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setFile(file);
        }
        return this;
    }

    public DagBuilder file() {
        if (this.connectionsList.size() != 0) {
            this.connectionsList.get(this.connectionsList.size() - 1).setFile(new File());
        }
        return this;
    }


    public DagBuilder area(String area) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getArea() == null) {
                this.tablesList.get(tablesList.size() - 1).setArea(area);
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getArea() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setArea(area);
                    }
            }
            }
        }
        return this;
    }

    public DagBuilder vertical(String vertical) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getVertical() == null) {
                this.tablesList.get(tablesList.size() - 1).setVertical(vertical);
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getVertical() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setVertical(vertical);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder table(String table) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getTable() == null) {
                this.tablesList.get(tablesList.size() - 1).setTable(table);
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getTable() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setTable(table);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder version(String version) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getVersion() == null) {
                this.tablesList.get(tablesList.size() - 1).setVersion(version);
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size()-1).size() != 0){
                    if (this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).getVersion() == null) {
                        this.sourcesList.get(tablesList.size()-1).get(sourcesList.get(tablesList.size()-1).size() - 1).setVersion(version);
                    }
                }
            }

            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).getVersion() == null) {
                        this.dependenciesList.get(tablesList.size()-1).get(dependenciesList.get(tablesList.size()-1).size() - 1).setVersion(version);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder parallelism(int parallelism) {
        if (this.tablesList.size() != 0) {
            this.tablesList.get(tablesList.size()-1).setParallelism(parallelism);
        }
        return this;
    }

    public DagBuilder maxBulkSize(int maxBulkSize) {
        if (this.tablesList.size() != 0) {
            this.tablesList.get(tablesList.size()-1).setMax_bulk_size(maxBulkSize);
        }
        return this;
    }

    public DagBuilder partition(Partition partition) {
        if(this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getPartition() == null) {
                this.tablesList.get(tablesList.size() - 1).setPartition(partition);
            }

            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size()-1).size() != 0){
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() == null) {
                        this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPartition(partition);
                    }
                }
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() == null) {
                            this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setPartition(partition);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder partition() {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size()-1).getPartition() == null) {
                Partition partition = new Partition();
                this.tablesList.get(tablesList.size()-1).setPartition(partition);
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size()-1).size() != 0){
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() == null) {
                        Partition partition = new Partition();
                        this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPartition(partition);

                    }
                }
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size()-1).size() != 0){
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() == null) {
                            Partition partition = new Partition();
                            this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setPartition(partition);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder unit(String unit) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getPartition() != null) {
                if (this.tablesList.get(tablesList.size() - 1).getPartition().getUnit() == null) {
                    this.tablesList.get(tablesList.size() - 1).getPartition().setUnit(unit);
                }
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition().getUnit() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition().setUnit(unit);
                        }
                    }
                }
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() != null) {
                            if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition().getUnit() == null) {
                                this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition().setUnit(unit);
                            }
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder size(int size) {
        if (this.tablesList.size() != 0) {
            if (this.tablesList.get(tablesList.size() - 1).getPartition() != null) {
                if (this.tablesList.get(tablesList.size() - 1).getPartition().getSize() == null) {
                    this.tablesList.get(tablesList.size() - 1).getPartition().setSize(size);
                }
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition().getSize() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getPartition().setSize(size);
                        }
                    }
                }
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition() != null) {
                        if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition().getSize() == null) {
                            this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().getPartition().setSize(size);
                        }
                    }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder sources(Source[] sources) {
        if (this.tablesList.size() != 0)  {
            this.tablesList.get(tablesList.size() - 1).setSources(sources);
        }
        return this;
    }

    public DagBuilder source() {
            if (this.tablesList.size() != 0)  {
                this.sourcesList.get(tablesList.size() - 1).add(new Source());
            }
        return this;
    }

    public DagBuilder targets(Target[] targets) {
        if (this.tablesList.size() != 0) {
            this.tablesList.get(tablesList.size() - 1).setTargets(targets);
        }
        return this;
    }

    public DagBuilder target() {
        if (this.tablesList.size() != 0) {
            this.targetsList.get(tablesList.size() - 1).add(new Target());
        }
        return this;
    }

    public DagBuilder dependencies(Dependency[] dependencies) {
        if (this.tablesList.size() != 0) {
            this.tablesList.get(tablesList.size() - 1).setDependencies(dependencies);
        }
        return this;
    }

    public DagBuilder dependency() {
        if (this.tablesList.size() != 0) {
            this.dependenciesList.get(tablesList.size() - 1).add(new Dependency());
        }
        return this;
    }

    public DagBuilder connectionName(String connection_name) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getConnection_name() == null) {
                        this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setConnection_name(connection_name);
                    }
                }
            }
            if (this.targetsList.size() != 0) {
                if (this.targetsList.get(tablesList.size() - 1).size() != 0) {
                    if (this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).getConnection_name() == null) {
                        this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).setConnection_name(connection_name);
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder path(String path) {
        if (this.connectionsList.size() != 0){
            if (this.connectionsList.get(this.connectionsList.size()-1).getS3() != null) {
                if (this.connectionsList.get(this.connectionsList.size() - 1).getS3().getPath() == null) {
                    this.connectionsList.get(this.connectionsList.size() - 1).getS3().setPath(path);
                }
            }
            if (this.connectionsList.get(this.connectionsList.size()-1).getFile() != null) {
                if (this.connectionsList.get(this.connectionsList.size() - 1).getFile().getPath() == null) {
                    this.connectionsList.get(this.connectionsList.size() - 1).getFile().setPath(path);
                }
            }
        }
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setPath(path);
                }
            }
        }
        return this;
    }


    public DagBuilder format(String format) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                        this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                    }
                }
            }
            if (this.targetsList.size() != 0) {
                if (this.targetsList.get(tablesList.size() - 1).size() != 0) {
                    if (this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                        this.targetsList.get(tablesList.size() - 1).get(targetsList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                    }
                }
            }
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getFormat() == null) {
                        this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).setFormat(format);
                    }
                }
            }
        }
        return this;
    }


    public DagBuilder parallelLoad(ParallelLoad parallel_load) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setParallel_load(parallel_load);
                }
            }
        }
        return this;
    }

    public DagBuilder parallelLoad() {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setParallel_load(new ParallelLoad());
                }
            }
        }
        return this;
    }


    public DagBuilder query(String query) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().getQuery() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().setQuery(query);
                        }
                    }
                }
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect().getQuery() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect().setQuery(query);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder column(String column) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().getColumn() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().setColumn(column);
                        }
                    }
                }
            }
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect().getColumn() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getSelect().setColumn(column);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder num(int num) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load() != null) {
                        if (this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().getNum() == null) {
                            this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).getParallel_load().setNum(num);
                        }
                    }
                }
            }
        }
        return this;
    }

    public DagBuilder select(Select select) {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setSelect(select);
                }
            }
        }
        return this;
    }

    public DagBuilder select() {
        if (this.tablesList.size() != 0) {
            if (this.sourcesList.size() != 0) {
                if (this.sourcesList.get(tablesList.size() - 1).size() != 0) {
                    this.sourcesList.get(tablesList.size() - 1).get(sourcesList.get(tablesList.size() - 1).size() - 1).setSelect(new Select());
                }
            }
        }
        return this;
    }


    public DagBuilder transform(Transform transform) {
        if (this.tablesList.size() != 0) {
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).setTransform(transform);
                }
            }
        }
        return this;
    }

    public DagBuilder transform() {
        if (this.tablesList.size() != 0) {
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).setTransform(new Transform());
                }
            }
        }
        return this;
    }

    public DagBuilder type(String type) {
        if (this.tablesList.size() != 0) {
            if (this.dependenciesList.size() != 0) {
                if (this.dependenciesList.get(tablesList.size() - 1).size() != 0) {
                    if (this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform() != null) {
                        this.dependenciesList.get(tablesList.size() - 1).get(dependenciesList.get(tablesList.size() - 1).size() - 1).getTransform().setType(type);
                    }
                }
            }
        }
        return this;
    }

    public Dag build() {
        if (tablesList != null) {
            int tablesLength = tablesList.size();
            if (tablesLength != 0) {
                for (int i = 0; i < tablesLength; i++) {
                    if (this.sourcesList.get(i) != null) {
                        Source[] sources = new Source[sourcesList.get(i).size()];
                        sourcesList.get(i).toArray(sources);
                        this.tablesList.get(i).setSources(sources);
                    }
                    if (this.targetsList.get(i) != null) {
                        Target[] targets = new Target[targetsList.get(i).size()];
                        targetsList.get(i).toArray(targets);
                        this.tablesList.get(i).setTargets(targets);
                    }
                    if (this.dependenciesList.get(i) != null) {
                        Dependency[] dependencies = new Dependency[dependenciesList.get(i).size()];
                        dependenciesList.get(i).toArray(dependencies);
                        this.tablesList.get(i).setDependencies(dependencies);
                    }
                }
                this.tables = new Table[tablesList.size()];
                tablesList.toArray(this.tables);
                this.dag.setTables(this.tables);
            }
        }
        if (this.connectionsList != null) {
            this.connections = new Connection[connectionsList.size()];
            connectionsList.toArray(this.connections);
            this.dag.setConnections(this.connections);
        }
        YamlConfigReader configReader = new YamlConfigReader();
        return configReader.getModelDag(this.dag);
    }
}
